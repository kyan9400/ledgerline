"""Real-time fraud features, kept per customer and per merchant from the payment events.

The same code computes them online (the scorer, from Kafka and from each request it scores) and offline
(training, replaying the topic), so the model is trained on exactly what it will see. A payment's
features are computed from the state *before* it, then the payment is applied.

Times are seconds (floats). Amounts are minor units (kopecks), as everywhere in Ledgerline.
"""

from __future__ import annotations

import math
import threading
from collections import deque
from dataclasses import dataclass, field

HOUR = 3600.0
SMALL = 1_000  # under 10 roubles: the size of a card test

NAMES = [
    "n_1m", "n_10m", "n_1h",              # the customer's attempts in the last minute, 10 minutes, hour
    "sum_10m_log", "sum_1h_log",          # and their total amounts (log)
    "merchants_10m",                      # distinct merchants in 10 minutes
    "declines_10m",                       # the customer's declined attempts in 10 minutes
    "small_10m",                          # attempts under 10 roubles in 10 minutes
    "history_log",                        # how many attempts the customer has made before (log)
    "amount_log",                         # this amount (log)
    "amount_z",                           # this amount against the customer's usual (log scale)
    "amount_vs_max",                      # this amount over the largest before
    "new_merchant",                       # never paid this merchant before
    "merchant_share",                     # the share of the customer's attempts at this merchant
    "since_last_log",                     # seconds since the customer's last attempt (log)
    "merchant_n_10m",                     # the merchant's attempts in 10 minutes
]


@dataclass
class Customer:
    recent: deque = field(default_factory=deque)      # (t, amount, merchant), the last hour
    declines: deque = field(default_factory=deque)    # t, the last hour
    n: int = 0
    mean: float = 0.0                                 # Welford's, over log amounts
    m2: float = 0.0
    max_amount: int = 0
    merchants: dict = field(default_factory=dict)     # merchant -> attempts
    last: float | None = None


class Features:
    """Thread-safe: the Kafka consumer and the request handlers share it."""

    def __init__(self, seen_limit: int = 2_000_000):
        self.customers: dict[str, Customer] = {}
        self.merchants: dict[str, deque] = {}
        self.seen: dict[str, None] = {}  # payment ids applied (an event and its request are one attempt)
        self.declined: dict[str, None] = {}
        self.seen_limit = seen_limit
        self.lock = threading.Lock()

    # --- reading -------------------------------------------------------------------------------------

    def compute(self, customer: str, merchant: str, amount: int, t: float) -> list[float]:
        with self.lock:
            c = self.customers.get(customer) or Customer()
            _prune(c.recent, t, key=lambda e: e[0])
            _prune(c.declines, t, key=lambda e: e)
            m = self.merchants.get(merchant) or deque()
            _prune(m, t, key=lambda e: e, window=600)

            def since(seconds: float):
                return [e for e in c.recent if t - e[0] <= seconds]

            r1, r10 = since(60), since(600)
            log_amount = math.log1p(amount)
            std = math.sqrt(c.m2 / (c.n - 1)) if c.n > 1 else 0.0
            z = (log_amount - c.mean) / max(std, 0.3) if c.n >= 3 else 0.0
            return [
                len(r1), len(r10), len(c.recent),
                math.log1p(sum(e[1] for e in r10)), math.log1p(sum(e[1] for e in c.recent)),
                len({e[2] for e in r10}),
                sum(1 for d in c.declines if t - d <= 600),
                sum(1 for e in r10 if e[1] < SMALL),
                math.log1p(c.n),
                log_amount,
                z,
                amount / c.max_amount if c.max_amount else 0.0,
                0.0 if merchant in c.merchants else 1.0,
                c.merchants.get(merchant, 0) / c.n if c.n else 0.0,
                math.log1p(min(t - c.last, HOUR)) if c.last is not None else math.log1p(HOUR),
                len(m),
            ]

    # --- applying ------------------------------------------------------------------------------------

    def attempt(self, payment: str, customer: str, merchant: str, amount: int, t: float) -> bool:
        """An authorisation attempt. Once per payment: False if it was applied already."""
        with self.lock:
            if payment in self.seen:
                return False
            self._remember(self.seen, payment)
            c = self.customers.setdefault(customer, Customer())
            c.recent.append((t, amount, merchant))
            c.n += 1
            x = math.log1p(amount)
            delta = x - c.mean
            c.mean += delta / c.n
            c.m2 += delta * (x - c.mean)
            c.max_amount = max(c.max_amount, amount)
            c.merchants[merchant] = c.merchants.get(merchant, 0) + 1
            c.last = t if c.last is None else max(c.last, t)
            self.merchants.setdefault(merchant, deque()).append(t)
            return True

    def decline(self, payment: str, customer: str, t: float) -> None:
        with self.lock:
            if payment in self.declined:
                return
            self._remember(self.declined, payment)
            self.customers.setdefault(customer, Customer()).declines.append(t)

    def _remember(self, d: dict, key: str) -> None:
        d[key] = None
        if len(d) > self.seen_limit:
            for k in list(d)[: self.seen_limit // 10]:
                del d[k]


def _prune(q: deque, t: float, key, window: float = HOUR) -> None:
    while q and t - key(q[0]) > window:
        q.popleft()


# The rules' thresholds: what an analyst would start from. train.py tunes them on the training data, to the
# same budget of genuine declines as the model, and keeps them in model.json.
RULES = {"n_1m": 4, "small_10m": 4, "merchants_10m": 5, "amount_z": 2.5}


def rules(f: list[float], p: dict | None = None) -> bool:
    """The baseline the model must beat: what a fraud analyst would write first."""
    p = p or RULES
    v = dict(zip(NAMES, f))
    return (
        v["n_1m"] >= p["n_1m"]
        or v["small_10m"] >= p["small_10m"]
        or v["merchants_10m"] >= p["merchants_10m"]
        or (v["new_merchant"] == 1 and v["amount_z"] > p["amount_z"] and v["history_log"] >= math.log1p(5))
    )


def apply_event(features: Features, event: dict) -> None:
    """A payments.events event, as the scorer and training see it."""
    kind = event.get("type", "")
    data = event.get("data") or {}
    if not kind.startswith("payment."):
        return
    pid = data.get("id")
    if kind == "payment.authorising":
        features.attempt(pid, data["customer"], data["merchant"], int(data["amount"]), epoch(data["createdAt"]))
    elif kind == "payment.declined":
        features.decline(pid, data["customer"], epoch(data["updatedAt"]))


def epoch(iso: str) -> float:
    """An ISO-8601 instant (as Java writes them, up to nanoseconds) in seconds."""
    from datetime import datetime, timezone

    s = iso.rstrip("Z")
    if "." in s:
        whole, frac = s.split(".", 1)
        s = f"{whole}.{frac[:6]}"
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()
