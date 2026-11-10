"""Customers who pay, fraudsters who don't own the cards they pay with, and the truth about which is which.

It drives the real payments API (authorise, then capture what is authorised), and writes one line per
attempt to a truth file: the payment id, whether it was fraud, and which pattern. Nothing in the
payments system sees the truth; the scorer is measured against it afterwards.

**Customers** have habits: a few favourite merchants (most of their payments), a usual amount (each a
log-normal around their own median), and a pace (about one attempt a minute; time here is fast). Now
and then they do something unusual, as real customers do: a new merchant, or a splurge (several times
their usual, somewhere new). Those are the false alarms a good model must not raise.

**Fraud**, in episodes, on a customer whose card has been stolen:

- card testing: a string of tiny payments at random merchants, seconds apart, to see whether the card
  works, then a large one or two;
- account takeover: several large payments at merchants the customer has never used, close together;
- a single large hit: one payment, several times the usual, somewhere new. Alone it looks like a
  splurge: the hard case.

Environment: PAYMENTS, LEDGER, MINUTES=20, CUSTOMERS=400, MERCHANTS=60, EPISODE_EVERY_S=15, SEED, OUT.
"""

from __future__ import annotations

import json
import math
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib import error, request

PAYMENTS = os.environ.get("PAYMENTS", "http://payments:8080")
LEDGER = os.environ.get("LEDGER", "http://ledger:8080")


def call(method: str, url: str, body: dict | None = None, key: str | None = None, tries: int = 30) -> tuple[int, dict]:
    """With retries (same key) on no answer or a 5xx: the client behaviour Ledgerline expects."""
    data = json.dumps(body).encode() if body is not None else (b"" if method == "POST" else None)
    for i in range(tries):
        req = request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        if key:
            req.add_header("Idempotency-Key", key)
        try:
            with request.urlopen(req, timeout=10) as res:
                return res.status, json.loads(res.read() or b"{}")
        except error.HTTPError as e:
            if e.code < 500:
                return e.code, json.loads(e.read() or b"{}")
        except (error.URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(min(2.0, 0.1 * 2 ** i) * (0.5 + random.random()))
    raise RuntimeError(f"{method} {url}: no answer")


class Sim:
    def __init__(self, seed: int, customers: int, merchants: int, out: str):
        self.r = random.Random(seed)
        self.run = f"sim-{int(time.time() * 1000):x}"
        self.out = open(out, "a", buffering=1)
        self.lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=64)
        self.attacked: set[str] = set()
        bank = self.open(f"{self.run}:bank", "system", negative=True)
        self.merchants = [self.open(f"{self.run}:m{i}", "merchant") for i in range(merchants)]
        self.customers = []
        for i in range(customers):
            cid = self.open(f"{self.run}:c{i}", "customer")
            call("POST", f"{LEDGER}/v1/transfers", {"from": bank, "to": cid, "amount": 1_000_000_000}, key=str(uuid.uuid4()))
            self.customers.append({
                "id": cid,
                "favourites": self.r.sample(self.merchants, self.r.randint(3, 8)),
                "median": int(math.exp(self.r.uniform(math.log(20_000), math.log(400_000)))),  # 200 to 4,000 roubles
                "pace": self.r.uniform(30, 120),  # seconds between attempts, on average
            })

    def open(self, code: str, kind: str, negative: bool = False) -> str:
        s, b = call("POST", f"{LEDGER}/v1/accounts", {"code": code, "currency": "RUB", "kind": kind, "allowNegative": negative}, key=str(uuid.uuid4()))
        assert s == 201, b
        return b["id"]

    def pay(self, customer: dict, merchant: str, amount: int, fraud: bool, pattern: str, episode: str | None) -> None:
        key = str(uuid.uuid4())
        started = time.time()
        status, body = call("POST", f"{PAYMENTS}/v1/payments", {"customer": customer["id"], "merchant": merchant, "amount": amount}, key=key)
        ms = (time.time() - started) * 1000
        pid = body.get("id")
        state = body.get("state")
        if status in (200, 201, 202) and pid:
            # Wait out a 202 (the step finishing later), then capture what was authorised.
            for _ in range(50):
                if state != "authorising":
                    break
                time.sleep(0.2)
                state = call("GET", f"{PAYMENTS}/v1/payments/{pid}")[1].get("state")
            if state == "authorised":
                call("POST", f"{PAYMENTS}/v1/payments/{pid}/capture", {}, key=str(uuid.uuid4()))
        row = {
            "at": started, "payment": pid, "customer": customer["id"], "merchant": merchant, "amount": amount,
            "fraud": fraud, "pattern": pattern, "episode": episode, "status": status, "state": state,
            "decline_reason": body.get("declineReason"), "score": body.get("fraudScore"),
            "decision": body.get("fraudDecision"), "ms": round(ms, 1),
        }
        with self.lock:
            self.out.write(json.dumps(row) + "\n")

    def legit(self, c: dict) -> None:
        roll = self.r.random()
        if roll < 0.03:  # a splurge: several times the usual, somewhere new
            m, amount, pattern = self.r.choice(self.merchants), int(c["median"] * self.r.uniform(4, 10)), "splurge"
        elif roll < 0.18:  # somewhere new, as usual
            m, amount, pattern = self.r.choice(self.merchants), self.amount(c), "new_merchant"
        else:
            m, amount, pattern = self.r.choice(c["favourites"]), self.amount(c), "usual"
        self.pool.submit(self.pay, c, m, amount, False, pattern, None)

    def amount(self, c: dict) -> int:
        return max(100, int(c["median"] * math.exp(self.r.gauss(0, 0.5))))

    def episode(self, c: dict) -> None:
        """Runs in its own thread: an attack takes a while."""
        eid = uuid.uuid4().hex[:12]
        r = random.Random(self.r.random())
        roll = r.random()
        strangers = [m for m in self.merchants if m not in c["favourites"]]
        try:
            if roll < 0.4:
                for _ in range(r.randint(6, 12)):
                    self.pay(c, r.choice(strangers), r.randint(100, 500), True, "card_testing", eid)
                    time.sleep(r.uniform(1, 4))
                for _ in range(r.randint(1, 2)):
                    self.pay(c, r.choice(strangers), int(c["median"] * r.uniform(5, 10)), True, "card_testing", eid)
                    time.sleep(r.uniform(1, 3))
            elif roll < 0.8:
                for _ in range(r.randint(3, 6)):
                    self.pay(c, r.choice(strangers), int(c["median"] * r.uniform(4, 12)), True, "takeover", eid)
                    time.sleep(r.uniform(3, 10))
            else:
                self.pay(c, r.choice(strangers), int(c["median"] * r.uniform(6, 15)), True, "single_large", eid)
        finally:
            with self.lock:
                self.attacked.discard(c["id"])

    def go(self, minutes: float, episode_every: float) -> None:
        end = time.time() + minutes * 60
        next_due = {c["id"]: time.time() + self.r.expovariate(1 / c["pace"]) for c in self.customers}
        next_episode = time.time() + self.r.expovariate(1 / episode_every)
        while time.time() < end:
            now = time.time()
            for c in self.customers:
                if next_due[c["id"]] <= now:
                    self.legit(c)
                    next_due[c["id"]] = now + self.r.expovariate(1 / c["pace"])
            if next_episode <= now:
                with self.lock:
                    free = [c for c in self.customers if c["id"] not in self.attacked]
                    victim = self.r.choice(free)
                    self.attacked.add(victim["id"])
                threading.Thread(target=self.episode, args=(victim,), daemon=True).start()
                next_episode = now + self.r.expovariate(1 / episode_every)
            time.sleep(0.05)
        time.sleep(60)  # the last episodes finish
        self.pool.shutdown(wait=True)


def main() -> None:
    sim = Sim(
        seed=int(os.environ.get("SEED", "1")),
        customers=int(os.environ.get("CUSTOMERS", "400")),
        merchants=int(os.environ.get("MERCHANTS", "60")),
        out=os.environ.get("OUT", "/out/truth.jsonl"),
    )
    print(f"{sim.run}: {len(sim.customers)} customers, {len(sim.merchants)} merchants", flush=True)
    sim.go(float(os.environ.get("MINUTES", "20")), float(os.environ.get("EPISODE_EVERY_S", "15")))
    print("done", flush=True)


if __name__ == "__main__":
    main()
