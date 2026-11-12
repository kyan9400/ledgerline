#!/usr/bin/env python3
"""Prints traces from Tempo as trees: each span with its service, its start (from the trace's first) and
its duration. Run where Tempo is reachable (or through docker compose run, see below).

    python3 scripts/trace.py <trace id>                     one trace
    python3 scripts/trace.py --search '<TraceQL>' [n]       the first n traces matching (default 3)

    docker run --rm --network ledgerline_internal -v "$PWD/scripts:/s:ro" python:3.13.15-slim \
      python3 /s/trace.py --search '{ name = "payment step" }'

TEMPO: Tempo's HTTP API (default http://tempo:3200).
"""

from __future__ import annotations

import base64
import json
import os
import sys
import urllib.parse
import urllib.request

TEMPO = os.environ.get("TEMPO", "http://tempo:3200")


def get(path: str) -> dict:
    with urllib.request.urlopen(f"{TEMPO}{path}", timeout=30) as r:
        return json.load(r)


def hex_id(b64: str) -> str:
    return base64.b64decode(b64).hex()


def attrs(span: dict) -> dict:
    out = {}
    for a in span.get("attributes", []):
        v = a["value"]
        out[a["key"]] = next(iter(v.values())) if v else None
    return out


def tree(trace_id: str) -> str:
    data = get(f"/api/v2/traces/{trace_id}")
    spans = []
    for batch in data.get("trace", data).get("resourceSpans", []):
        service = next((next(iter(a["value"].values())) for a in batch["resource"]["attributes"] if a["key"] == "service.name"), "?")
        for scope in batch.get("scopeSpans", []):
            for s in scope.get("spans", []):
                spans.append({
                    "id": hex_id(s["spanId"]), "parent": hex_id(s["parentSpanId"]) if s.get("parentSpanId") else None,
                    "name": s["name"], "service": service, "start": int(s["startTimeUnixNano"]),
                    "end": int(s["endTimeUnixNano"]), "error": s.get("status", {}).get("code") == "STATUS_CODE_ERROR",
                    "attrs": attrs(s),
                })
    if not spans:
        return f"{trace_id}: no spans"
    t0 = min(s["start"] for s in spans)
    ids = {s["id"] for s in spans}
    children: dict[str | None, list] = {}
    for s in spans:
        children.setdefault(s["parent"] if s["parent"] in ids else None, []).append(s)
    lines = [f"trace {trace_id}: {len(spans)} spans over {(max(s['end'] for s in spans) - t0) / 1e9:.3f} s"]

    def walk(parent: str | None, depth: int) -> None:
        for s in sorted(children.get(parent, []), key=lambda x: x["start"]):
            extra = ", ".join(f"{k}={v}" for k, v in s["attrs"].items()
                              if k in ("http.response.status_code", "status", "outcome", "fraud.decision", "messaging.message.id",
                                       "ledgerline.outbox.wait_ms", "ledgerline.webhook.outcome", "grpc.status_code", "peer.service"))
            lines.append(f"{'  ' * depth}{'! ' if s['error'] else ''}{s['service']}: {s['name']}"
                         f"  +{(s['start'] - t0) / 1e6:.0f} ms, {(s['end'] - s['start']) / 1e6:.1f} ms" + (f"  ({extra})" if extra else ""))
            walk(s["id"], depth + 1)

    walk(None, 1)
    return "\n".join(lines)


def main() -> None:
    if len(sys.argv) >= 3 and sys.argv[1] == "--search":
        n = int(sys.argv[3]) if len(sys.argv) > 3 else 3
        found = get(f"/api/search?{urllib.parse.urlencode({'q': sys.argv[2], 'limit': n})}").get("traces", [])
        print("\n\n".join(tree(t["traceID"]) for t in found[:n]) or "no traces found")
    elif len(sys.argv) == 2:
        print(tree(sys.argv[1]))
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
