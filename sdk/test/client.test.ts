// The client against an in-process gRPC server speaking the same .proto: retries keep the key, refusals
// carry their code, the stream resumes from its cursor.
import { strict as assert } from "node:assert";
import { existsSync } from "node:fs";
import { after, before, test } from "node:test";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as loader from "@grpc/proto-loader";
import { Ledgerline, LedgerlineError } from "../src/index.ts";

const here = dirname(fileURLToPath(import.meta.url));
// The SDK's copy of the .proto (npm run proto), or the repository's.
const protoDir = [join(here, "..", "proto"), join(here, "..", "..", "proto")].find((d) => existsSync(join(d, "ledgerline", "v1", "merchants.proto")))!;
const definition = loader.loadSync("ledgerline/v1/merchants.proto", { includeDirs: [protoDir], keepCase: false, longs: Number, defaults: true, oneofs: true });
const pkg = grpc.loadPackageDefinition(definition) as any;

const keys: string[] = [];
const watchFrom: string[] = [];
let authoriseCalls = 0;
const payment = (id: string, version: number, state = "authorised") => ({ id, state, version, amount: 100, createdAt: { seconds: 1, nanos: 0 } });
const server = new grpc.Server();
server.addService(pkg.ledgerline.v1.Merchants.service, {
  authorise(call: any, cb: any) {
    keys.push(String(call.metadata.get("idempotency-key")[0]));
    // The first two tries "time out": the client must send again, with the same key.
    if (++authoriseCalls <= 2) return cb({ code: grpc.status.UNAVAILABLE, details: "restarting" });
    cb(null, payment("p1", 2));
  },
  capture(_call: any, cb: any) {
    const trailers = new grpc.Metadata();
    trailers.set("ledgerline-error-code", "invalid_state");
    cb({ code: grpc.status.FAILED_PRECONDITION, details: "payment p1 is voided", metadata: trailers });
  },
  watchPayments(call: any) {
    watchFrom.push(call.request.from);
    if (call.request.from === "") {
      call.write({ id: "payment-p1:1", type: "payment.authorising", cursor: "c1", payment: payment("p1", 1, "authorising") });
      call.emit("error", { code: grpc.status.UNAVAILABLE, details: "Kafka went away" }); // the stream breaks
    } else {
      call.write({ id: "payment-p1:2", type: "payment.authorised", cursor: "c2", payment: payment("p1", 2) });
      call.end();
    }
  },
});
let client: Ledgerline;

before(async () => {
  const port = await new Promise<number>((ok, fail) =>
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (e, p) => (e ? fail(e) : ok(p))));
  client = new Ledgerline({ target: `127.0.0.1:${port}`, apiKey: "ll_sk_test" });
});
after(() => {
  client.close();
  server.forceShutdown();
});

test("a write with no answer is sent again, with the same idempotency key", async () => {
  const p = await client.authorise({ customer: "c", amount: 100 });
  assert.equal(p.id, "p1");
  assert.equal(authoriseCalls, 3);
  assert.equal(new Set(keys).size, 1, `one key for all three tries: ${keys}`);
});

test("a refusal isn't retried, and carries Ledgerline's code", async () => {
  await assert.rejects(client.capture("p1"), (e: LedgerlineError) => e.code === "invalid_state" && !e.retryable && e.grpcCode === grpc.status.FAILED_PRECONDITION);
});

test("a broken stream resumes from the last event's cursor", async () => {
  const seen: string[] = [];
  for await (const e of client.watch()) {
    seen.push(e.id);
    if (seen.length === 2) break;
  }
  assert.deepEqual(seen, ["payment-p1:1", "payment-p1:2"]);
  assert.deepEqual(watchFrom, ["", "c1"]);
});
