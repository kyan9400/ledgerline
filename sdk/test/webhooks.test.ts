import { strict as assert } from "node:assert";
import { test } from "node:test";
import { signWebhook, verifyWebhook, WebhookSignatureError } from "../src/index.ts";

const secret = "ll_whsec_test";
const body = JSON.stringify({ id: "payment-1:2", type: "payment.authorised", version: 2, created: "2026-11-11T00:00:00Z", data: { id: "1" } });
const now = 1_794_000_000_000;
const t = now / 1000;

test("a correctly signed webhook gives its event", () => {
  const e = verifyWebhook(body, signWebhook(body, secret, t), secret, { now });
  assert.equal(e.id, "payment-1:2");
  assert.equal(e.version, 2);
});

test("a changed body, another secret, or no signature is refused", () => {
  const header = signWebhook(body, secret, t);
  assert.throws(() => verifyWebhook(body.replace("authorised", "captured"), header, secret, { now }), WebhookSignatureError);
  assert.throws(() => verifyWebhook(body, header, "ll_whsec_other", { now }), WebhookSignatureError);
  assert.throws(() => verifyWebhook(body, undefined, secret, { now }), WebhookSignatureError);
  assert.throws(() => verifyWebhook(body, "t=1,v1=zz", secret, { now }), WebhookSignatureError);
});

test("an old signature is refused, so a recorded webhook can't be replayed", () => {
  assert.throws(() => verifyWebhook(body, signWebhook(body, secret, t - 301), secret, { now }), /too old/);
  assert.doesNotThrow(() => verifyWebhook(body, signWebhook(body, secret, t - 299), secret, { now }));
});

test("it signs exactly as the server does (the same value is pinned in WebhooksTest.kt)", () => {
  const header = signWebhook('{"id":"payment-1:2"}', secret, 1_794_000_000);
  assert.equal(header, "t=1794000000,v1=3a1acea683c891b811cf0db1e8214daa2d539e0dee34471d955b0a8d2f044450");
});
