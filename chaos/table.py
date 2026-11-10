"""Prints the README's chaos table from eval/chaos/*.json (the reports chaos/run.sh writes)."""
import json
import pathlib

HERE = pathlib.Path(__file__).resolve().parent.parent / "eval" / "chaos"
SCENARIOS = [
    ("baseline", "nothing"),
    ("kill-ledger", "the ledger killed (SIGKILL) 3 times, 10 s each"),
    ("kill-payments", "the payments service killed 3 times, mid-step"),
    ("kill-postgres", "PostgreSQL killed twice: both services lose their database"),
    ("kill-fraud", "the fraud scorer killed twice, 15 s each"),
    ("kill-clickhouse", "ClickHouse killed twice mid-ingestion, 15 s each"),
    ("kill-kafka", "Kafka killed for 50 s"),
    ("partition", "the ledger, then the bank, cut off the network for 20 s"),
    ("toxic", "connections to the ledger slowed, cut mid-answer, reset, hung; the bank's hung"),
    ("kafka-loss", "Kafka loses all its data; refilled from the event logs"),
    ("mixed", "PostgreSQL, Kafka and payments killed; answers cut"),
]


def ms(v):
    return f"{v / 1000:.1f} s" if v >= 1000 else f"{v} ms"


print("| Scenario | What breaks | Requests (sent twice at once) | Failed tries, retried | Time to an answer: p50 / p95 / max | Verified |")
print("| --- | --- | --- | --- | --- | --- |")
total = twice = 0
for name, what in SCENARIOS:
    v = json.loads((HERE / f"{name}.json").read_text())
    w = json.loads((HERE / f"{name}.workload.json").read_text())
    m = w["ms_to_answer"]
    total += w["requests"]
    twice += w["sent_twice_at_once"]
    failed = sum(w["errors_seen"].values())
    print(
        f"| `{name}` | {what} | {w['requests']:,} ({w['sent_twice_at_once']}) | {failed:,} | "
        f"{ms(m['p50'])} / {ms(m['p95'])} / {ms(m['max'])} | {'all checks hold' if v['ok'] else 'FAILED'} |"
    )
print(f"\ntotal requests: {total:,}, sent twice at once: {twice:,}")
