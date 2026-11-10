"""Measures a live run against the truth: what the payments service declined as fraud, and what it
should have.

    python -m fraud.evaluate truth.jsonl [--report report.json]

Per attempt (the simulator's truth file): was it fraud, and was it declined as fraud_suspected. Per
fraud episode (one stolen card): how many of its attempts got through before the first decline, and the
money that got through. A genuine payment declined while the customer's card is under attack (a fraud
attempt on it within the last 10 minutes) is counted apart: the card was being used by a thief, and a
bank would block it then anyway.
"""

from __future__ import annotations

import argparse
import bisect
import json
import statistics
from collections import defaultdict

UNDER_ATTACK_S = 600


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(len(xs) * q))], 1) if xs else 0.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("truth")
    ap.add_argument("--report")
    args = ap.parse_args()
    rows = [json.loads(line) for line in open(args.truth)]
    rows = [r for r in rows if r.get("payment")]
    blocked = lambda r: r.get("decline_reason") == "fraud_suspected"  # noqa: E731
    fraud = [r for r in rows if r["fraud"]]
    genuine = [r for r in rows if not r["fraud"]]

    attacks = defaultdict(list)  # customer -> times of fraud attempts on their card
    for r in fraud:
        attacks[r["customer"]].append(r["at"])
    for ts in attacks.values():
        ts.sort()

    def under_attack(r) -> bool:
        ts = attacks.get(r["customer"], [])
        i = bisect.bisect_right(ts, r["at"])
        return i > 0 and r["at"] - ts[i - 1] <= UNDER_ATTACK_S

    def share(xs, f):
        return round(sum(1 for r in xs if f(r)) / max(1, len(xs)), 4)

    def money(xs, f):
        return round(sum(r["amount"] for r in xs if f(r)) / max(1, sum(r["amount"] for r in xs)), 4)

    by_pattern = defaultdict(list)
    for r in rows:
        by_pattern[r["pattern"]].append(r)
    episodes = defaultdict(list)
    for r in sorted(fraud, key=lambda r: r["at"]):
        episodes[r["episode"]].append(r)
    through_before_block, lost, at_risk, stopped = [], 0, 0, 0
    for attempts in episodes.values():
        first = next((i for i, r in enumerate(attempts) if blocked(r)), None)
        through_before_block.append(len(attempts) if first is None else first)
        lost += sum(r["amount"] for r in attempts if r["state"] in ("authorised", "captured") and not blocked(r))
        at_risk += sum(r["amount"] for r in attempts)
        stopped += first == 0
    calm = [r for r in genuine if not under_attack(r)]
    report = {
        "attempts": len(rows),
        "fraud_attempts": len(fraud),
        "unscored": sum(1 for r in rows if r.get("decision") == "unscored"),
        "fraud_blocked": share(fraud, blocked),
        "fraud_money_blocked": money(fraud, blocked),
        "genuine_declined_as_fraud": share(genuine, blocked),
        "genuine_money_declined_as_fraud": money(genuine, blocked),
        "genuine_while_card_under_attack": {"attempts": len(genuine) - len(calm), "declined_as_fraud": share([r for r in genuine if under_attack(r)], blocked)},
        "genuine_otherwise": {"attempts": len(calm), "declined_as_fraud": share(calm, blocked), "money_declined": money(calm, blocked)},
        "precision": round(sum(1 for r in fraud if blocked(r)) / max(1, sum(1 for r in rows if blocked(r))), 4),
        "by_pattern": {p: {"attempts": len(xs), "declined_as_fraud": share(xs, blocked)} for p, xs in sorted(by_pattern.items())},
        "episodes": {
            "count": len(episodes),
            "stopped_at_the_first_attempt": stopped,
            "attempts_through_before_the_first_block_median": statistics.median(through_before_block) if episodes else 0,
            "money_lost_share": round(lost / max(1, at_risk), 4),
        },
        "authorise_ms": {"p50": pct([r["ms"] for r in rows], 0.5), "p95": pct([r["ms"] for r in rows], 0.95), "p99": pct([r["ms"] for r in rows], 0.99)},
    }
    print(json.dumps(report, indent=1))
    if args.report:
        with open(args.report, "w") as fh:
            json.dump(report, fh, indent=1)


if __name__ == "__main__":
    main()
