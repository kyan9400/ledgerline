import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as loader from "@grpc/proto-loader";

/** Amounts are whole minor units (kopecks, cents). */
export interface Payment {
  id: string;
  state: "authorising" | "authorised" | "declined" | "capturing" | "captured" | "voiding" | "voided";
  customer: string;
  currency: string;
  amount: number;
  captured: number;
  refunded: number;
  declineReason: string;
  voidReason: string;
  description: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
  fraudScore?: number;
}

export interface Refund {
  id: string;
  paymentId: string;
  state: "refunding" | "refunded" | "failed";
  amount: number;
  failureReason: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface PaymentEvent {
  /** stream:version: the same id as the webhook for this change. */
  id: string;
  type: string;
  /** Pass to watch({ from }) to resume after this event. */
  cursor: string;
  recordedAt: Date;
  payment?: Payment;
  refund?: Refund;
}

export interface ClientOptions {
  /** host:port of the merchants' gRPC API. */
  target: string;
  apiKey: string;
  /**
   * How long to keep sending a write that gets no answer (with the same idempotency key), in ms. Long
   * enough by default (30 s) to ride out a restart of the service.
   */
  retryForMs?: number;
  /** Per attempt. Default 10 s. */
  timeoutMs?: number;
  credentials?: grpc.ChannelCredentials;
}

export interface WriteOptions {
  /** Yours, to make a retry of your own safe (an order id, say). One is made up when absent. */
  idempotencyKey?: string;
}

/** A refusal (or a failure after every retry). `code` is Ledgerline's stable code, when it gave one. */
export class LedgerlineError extends Error {
  readonly grpcCode: grpc.status;
  readonly code: string | undefined;
  readonly retryable: boolean;

  constructor(message: string, grpcCode: grpc.status, code: string | undefined, retryable: boolean) {
    super(message);
    this.grpcCode = grpcCode;
    this.code = code;
    this.retryable = retryable;
  }
}

const RETRYABLE = new Set([grpc.status.UNAVAILABLE, grpc.status.DEADLINE_EXCEEDED, grpc.status.RESOURCE_EXHAUSTED]);

function protoPath(): string {
  const here = dirname(fileURLToPath(import.meta.url));
  for (const dir of [join(here, "..", "proto"), join(here, "..", "..", "proto")]) {
    if (existsSync(join(dir, "ledgerline", "v1", "merchants.proto"))) return dir;
  }
  throw new Error("merchants.proto not found next to the SDK");
}

type Raw = Record<string, any>;

/**
 * Ledgerline for merchants.
 *
 * Every write carries an idempotency key (yours, or one made up), and a write that gets no answer (the
 * connection dropped, the service restarted) is sent again with the same key, backing off, so it happens
 * once. `watch()` follows your payments as they change, and picks up where it stopped if the stream
 * breaks.
 */
export class Ledgerline {
  private readonly stub: any;
  private readonly retryForMs: number;
  private readonly timeoutMs: number;
  private readonly apiKey: string;

  constructor(options: ClientOptions) {
    const definition = loader.loadSync("ledgerline/v1/merchants.proto", {
      includeDirs: [protoPath()], keepCase: false, longs: Number, enums: String, defaults: true, oneofs: true,
    });
    const pkg = grpc.loadPackageDefinition(definition) as any;
    this.stub = new pkg.ledgerline.v1.Merchants(options.target, options.credentials ?? grpc.credentials.createInsecure());
    this.retryForMs = options.retryForMs ?? 30_000;
    this.timeoutMs = options.timeoutMs ?? 10_000;
    this.apiKey = options.apiKey;
  }

  authorise(req: { customer: string; amount: number; description?: string }, options: WriteOptions = {}): Promise<Payment> {
    return this.write("authorise", req, options).then(payment);
  }

  /** All of the hold, or `amount` of it. */
  capture(paymentId: string, amount?: number, options: WriteOptions = {}): Promise<Payment> {
    return this.write("capture", amount === undefined ? { paymentId } : { paymentId, amount }, options).then(payment);
  }

  voidPayment(paymentId: string, options: WriteOptions = {}): Promise<Payment> {
    return this.write("voidPayment", { paymentId }, options).then(payment);
  }

  refund(paymentId: string, amount: number, options: WriteOptions = {}): Promise<Refund> {
    return this.write("refundPayment", { paymentId, amount }, options).then(refund);
  }

  getPayment(paymentId: string): Promise<Payment> {
    return this.unary("getPayment", { paymentId }, undefined).then(payment);
  }

  /** Newest first, a page at a time. */
  async listPayments(page: { pageSize?: number; pageToken?: string } = {}): Promise<{ payments: Payment[]; nextPageToken?: string }> {
    const r = await this.unary("listPayments", page, undefined);
    return { payments: r.payments.map(payment), nextPageToken: r.nextPageToken || undefined };
  }

  /**
   * Your payments' changes, as they happen (from now, or after `from`, an event's cursor). If the stream
   * breaks, it reconnects from the last event's cursor. An event may come twice: keep, per payment, the
   * last version handled.
   */
  async *watch(options: { from?: string; signal?: AbortSignal } = {}): AsyncGenerator<PaymentEvent> {
    let from = options.from ?? "";
    let failures = 0;
    while (!options.signal?.aborted) {
      const call = this.stub.watchPayments({ from }, this.metadata(undefined));
      const abort = () => call.cancel();
      options.signal?.addEventListener("abort", abort, { once: true });
      try {
        for await (const e of call as AsyncIterable<Raw>) {
          failures = 0;
          const ev = event(e);
          from = ev.cursor;
          yield ev;
        }
        return;
      } catch (err) {
        const code = (err as grpc.ServiceError).code;
        if (options.signal?.aborted || code === grpc.status.CANCELLED) return;
        if (!RETRYABLE.has(code) && code !== undefined) throw toError(err as grpc.ServiceError);
        await sleep(backoff(failures++));
      } finally {
        options.signal?.removeEventListener("abort", abort);
      }
    }
  }

  close(): void {
    this.stub.close();
  }

  // --- internals -----------------------------------------------------------------------------------

  private async write(method: string, req: Raw, options: WriteOptions): Promise<Raw> {
    const key = options.idempotencyKey ?? randomUUID();
    const giveUpAt = Date.now() + this.retryForMs;
    let attempt = 0;
    for (;;) {
      try {
        return await this.unary(method, req, key);
      } catch (err) {
        const e = err as LedgerlineError;
        if (!e.retryable || Date.now() >= giveUpAt) throw e;
        await sleep(backoff(attempt++));
      }
    }
  }

  private unary(method: string, req: Raw, idempotencyKey: string | undefined): Promise<Raw> {
    return new Promise((resolve, reject) => {
      this.stub[method](req, this.metadata(idempotencyKey), { deadline: Date.now() + this.timeoutMs }, (err: grpc.ServiceError | null, res: Raw) =>
        err ? reject(toError(err)) : resolve(res));
    });
  }

  private metadata(idempotencyKey: string | undefined): grpc.Metadata {
    const m = new grpc.Metadata();
    m.set("authorization", `Bearer ${this.apiKey}`);
    if (idempotencyKey) m.set("idempotency-key", idempotencyKey);
    return m;
  }
}

function toError(err: grpc.ServiceError): LedgerlineError {
  const code = err.metadata?.get("ledgerline-error-code")?.[0]?.toString();
  return new LedgerlineError(err.details || err.message, err.code, code, RETRYABLE.has(err.code));
}

const time = (t: Raw | null | undefined) => new Date(Number(t?.seconds ?? 0) * 1000 + Math.floor(Number(t?.nanos ?? 0) / 1e6));

function payment(p: Raw): Payment {
  return { ...p, createdAt: time(p.createdAt), updatedAt: time(p.updatedAt), fraudScore: p.fraudScore ?? undefined } as Payment;
}

function refund(r: Raw): Refund {
  return { ...r, createdAt: time(r.createdAt), updatedAt: time(r.updatedAt) } as Refund;
}

function event(e: Raw): PaymentEvent {
  return {
    id: e.id, type: e.type, cursor: e.cursor, recordedAt: time(e.recordedAt),
    payment: e.payment ? payment(e.payment) : undefined, refund: e.refund ? refund(e.refund) : undefined,
  };
}

/** 100 ms, doubling, at most 5 s, with jitter (clients retrying after an outage shouldn't all arrive at once). */
const backoff = (attempt: number) => Math.min(5_000, 100 * 2 ** attempt) * (0.5 + Math.random());
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
