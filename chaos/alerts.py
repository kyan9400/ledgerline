#!/usr/bin/env python3
"""The alert evaluation's table, from chaos/alerts.sh's events and timeline: per scenario, which alerts
fired, how long after the fault began, and how long after it ended they cleared; and any that fired
without being expected.

    python3 chaos/alerts.py eval/alerts      -> eval/alerts/results.json, results.md
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

EXPECTED = {
    "baseline": [],
    "ledger-down": ["ServiceDown", "PaymentsNotFinishing"],
    "kafka-down": ["OutboxLagging"],
    "fraud-down": ["ServiceDown", "FraudScorerFailingOpen"],
    "slow-ledger": ["DependencySlow"],
    "postgres-down": ["DatabaseUnreachable"],
    "webhooks-down": ["WebhooksFailing"],
    "books-wrong": ["LedgerBooksWrong"],
}

# What may follow from a scenario's fault: reported, but not a false alarm.
CONSEQUENCES = {
    "ledger-down": ["LoadShedding"],  # requests wait out the timeout to the ledger, and fill the service
    "slow-ledger": ["LoadShedding", "PaymentsNotFinishing", "ApiLatencyBudgetBurn"],
    # The slow burn (30 minutes and 6 hours) stays up for about half an hour after an outage: by design.
    "postgres-down": ["ApiErrorBudgetBurn", "ApiErrorBudgetBurnSlow", "ApiLatencyBudgetBurn", "LoadShedding", "PaymentsNotFinishing", "OutboxLagging"],
}


def main(out: Path) -> None:
    starts, ends, quiets = {}, {}, {}
    for line in (out / "events.log").read_text().splitlines():
        t, *words = line.split(" ")
        kind, name = words[0], words[-1]
        if kind == "start":
            starts[name] = int(t)
        elif kind == "end":
            ends[name] = int(t)
        elif kind == "quiet":
            quiets[name] = int(t)

    # (time, alertname, labels) for each poll at which an alert was firing.
    samples = []
    for line in (out / "timeline.log").read_text().splitlines():
        t, _, body = line.partition(" ")
        try:
            result = json.loads(body)["data"]["result"]
        except (ValueError, KeyError):
            continue
        for r in result:
            m = r["metric"]
            if m.get("alertstate") == "firing":
                labels = {k: v for k, v in m.items() if k not in ("__name__", "alertname", "alertstate", "severity", "instance")}
                samples.append((int(t), m["alertname"], m.get("severity", ""), json.dumps(labels, sort_keys=True)))

    rows = []
    for s, start in starts.items():
        end, until = ends.get(s, start), quiets.get(s, start)
        # Still firing from the scenario before (a long window's alert): not this fault's.
        carried = sorted({name for t, name, _, _ in samples if start - 60 <= t < start})
        fired: dict[str, dict] = {}
        for t, name, severity, labels in samples:
            if start <= t <= until and name not in carried:
                f = fired.setdefault(name, {"severity": severity, "first": t, "last": t, "labels": set()})
                f["first"], f["last"] = min(f["first"], t), max(f["last"], t)
                f["labels"].add(labels)
        expected = EXPECTED.get(s, [])
        rows.append({
            "scenario": s, "fault_seconds": end - start, "expected": expected,
            "missed": [a for a in expected if a not in fired],
            "carried_over": carried,
            "consequences": sorted(a for a in fired if a in CONSEQUENCES.get(s, [])),
            "unexpected": sorted(a for a in fired if a not in expected and a not in CONSEQUENCES.get(s, [])),
            "alerts": {a: {"severity": f["severity"], "fired_after_s": f["first"] - start,
                           "cleared_after_fault_s": max(0, f["last"] - end), "labels": sorted(f["labels"])}
                       for a, f in sorted(fired.items(), key=lambda x: x[1]["first"])},
        })

    ok = all(not r["missed"] and not r["unexpected"] for r in rows)
    (out / "results.json").write_text(json.dumps({"ok": ok, "scenarios": rows}, indent=2) + "\n")
    md = ["| Scenario | Fault | Alerts fired (after the fault began; cleared after it ended) | Expected, missed | Unexpected |",
          "|---|---|---|---|---|"]
    for r in rows:
        fired = "; ".join(f"**{a}** ({f['severity']}) {f['fired_after_s']} s; {f['cleared_after_fault_s']} s"
                          for a, f in r["alerts"].items()) or "none"
        md.append(f"| {r['scenario']} | {r['fault_seconds']} s | {fired} | {', '.join(r['missed']) or '-'} | "
                  f"{', '.join(r['unexpected']) or '-'} |")
    (out / "results.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))
    print("ok" if ok else "NOT OK")


if __name__ == "__main__":
    main(Path(sys.argv[1] if len(sys.argv) > 1 else "eval/alerts"))
