import { createHmac, timingSafeEqual } from "node:crypto";

/** A webhook, as Ledgerline sends it: one change to one of your payments or refunds. */
export interface WebhookEvent {
  /** stream:version, e.g. "payment-<id>:3". The same id as in the event stream. Handle each once. */
  id: string;
  /** payment.authorised, payment.captured, refund.refunded, ... */
  type: string;
  /** Grows with every change to the payment (or refund): delivery order isn't guaranteed, this is. */
  version: number;
  created: string;
  /** The payment (or refund) as it was after the change. */
  data: Record<string, unknown>;
}

export class WebhookSignatureError extends Error {}

/**
 * Checks a webhook's `Ledgerline-Signature` header, and returns the event it carries.
 *
 * The header is `t=<unix seconds>,v1=<hex HMAC-SHA256 of "t.body", keyed by your webhook secret>`. Pass
 * the body exactly as received (before any JSON parsing): the signature is over its bytes. A signature
 * older than `toleranceSeconds` is refused, so a recorded request can't be replayed later.
 */
export function verifyWebhook(
  body: string | Buffer,
  signatureHeader: string | undefined,
  secret: string,
  options: { toleranceSeconds?: number; now?: number } = {},
): WebhookEvent {
  if (!signatureHeader) throw new WebhookSignatureError("no Ledgerline-Signature header");
  const parts = new Map(signatureHeader.split(",").map((p) => {
    const i = p.indexOf("=");
    return [p.slice(0, i).trim(), p.slice(i + 1).trim()] as const;
  }));
  const t = Number(parts.get("t"));
  const v1 = parts.get("v1");
  if (!Number.isInteger(t) || !v1 || !/^[0-9a-f]{64}$/.test(v1)) throw new WebhookSignatureError("malformed Ledgerline-Signature header");
  const now = Math.floor((options.now ?? Date.now()) / 1000);
  if (Math.abs(now - t) > (options.toleranceSeconds ?? 300)) throw new WebhookSignatureError("signature too old (or from the future)");
  const raw = typeof body === "string" ? body : body.toString("utf8");
  const expected = createHmac("sha256", secret).update(`${t}.${raw}`).digest();
  if (!timingSafeEqual(expected, Buffer.from(v1, "hex"))) throw new WebhookSignatureError("signature doesn't match");
  return JSON.parse(raw) as WebhookEvent;
}

/** The signature header for a body: for tests, and for checking your own verification. */
export function signWebhook(body: string, secret: string, timestamp = Math.floor(Date.now() / 1000)): string {
  return `t=${timestamp},v1=${createHmac("sha256", secret).update(`${timestamp}.${body}`).digest("hex")}`;
}
