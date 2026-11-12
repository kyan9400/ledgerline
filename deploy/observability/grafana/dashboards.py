#!/usr/bin/env python3
"""Writes the Grafana dashboards (dashboards/*.json) from the definitions here: one place to change a
panel, and the JSON stays consistent. Run after editing: python3 deploy/observability/grafana/dashboards.py

- overview: is it working? The SLOs, what is firing, traffic, latency and errors per service, the
  service map (from the traces), and how the services' dependencies answer.
- payments: is the money moving? Steps, the work waiting, the outbox, webhooks, fraud, the books' audit,
  and the JVMs and connection pools underneath.
"""

from __future__ import annotations

import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "prometheus"}
TEMPO = {"type": "tempo", "uid": "tempo"}
HERE = Path(__file__).parent / "dashboards"

API = 'uri=~"/v1/.*"'


class Board:
    def __init__(self, uid: str, title: str, description: str):
        self.uid, self.title, self.description = uid, title, description
        self.panels: list[dict] = []
        self.x = self.y = 0
        self.row_h = 0

    def add(self, panel: dict, w: int, h: int) -> None:
        if self.x + w > 24:
            self.x, self.y = 0, self.y + self.row_h
            self.row_h = 0
        panel["gridPos"] = {"x": self.x, "y": self.y, "w": w, "h": h}
        panel["id"] = len(self.panels) + 1
        self.panels.append(panel)
        self.x += w
        self.row_h = max(self.row_h, h)

    def row(self, title: str) -> None:
        if self.x:
            self.x, self.y = 0, self.y + self.row_h
        self.panels.append({"type": "row", "title": title, "collapsed": False, "id": len(self.panels) + 1,
                            "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1}, "panels": []})
        self.y += 1
        self.row_h = 0

    def write(self) -> None:
        board = {
            "uid": self.uid, "title": self.title, "description": self.description, "tags": ["ledgerline"],
            "schemaVersion": 41, "version": 1, "editable": False, "graphTooltip": 1, "refresh": "30s",
            "time": {"from": "now-1h", "to": "now"}, "timezone": "utc", "panels": self.panels,
            "links": [{"title": "Dashboards", "type": "dashboards", "tags": ["ledgerline"], "asDropdown": False}],
            "templating": {"list": []}, "annotations": {"list": [{
                "name": "Alerts firing", "datasource": PROM, "enable": True, "iconColor": "red",
                "expr": 'ALERTS{alertstate="firing"}', "titleFormat": "{{alertname}}", "textFormat": "{{summary}}",
                "step": "30s", "useValueForTime": False,
            }]},
        }
        HERE.mkdir(exist_ok=True)
        (HERE / f"{self.uid}.json").write_text(json.dumps(board, indent=1) + "\n", encoding="utf-8")


def target(expr: str, legend: str = "", ref: str = "A", exemplar: bool = False, instant: bool = False) -> dict:
    t = {"datasource": PROM, "expr": expr, "legendFormat": legend or "__auto", "refId": ref, "exemplar": exemplar}
    if instant:
        t.update(instant=True, range=False, format="table")
    return t


def series(title: str, targets: list[dict], unit: str = "short", description: str = "", stack: bool = False,
           thresholds: list[tuple[float, str]] | None = None, minimum: float | None = 0) -> dict:
    custom = {"lineWidth": 1, "fillOpacity": 25 if stack else 8, "showPoints": "never", "spanNulls": True,
              "stacking": {"mode": "normal" if stack else "none"}}
    if thresholds:
        custom["thresholdsStyle"] = {"mode": "dashed"}
    defaults = {"unit": unit, "custom": custom}
    if minimum is not None:
        defaults["min"] = minimum
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": [{"value": None, "color": "transparent"}]
                                  + [{"value": v, "color": c} for v, c in thresholds]}
    return {"type": "timeseries", "title": title, "description": description, "datasource": PROM, "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi", "sort": "desc"}}}


def stat(title: str, expr: str, unit: str = "short", steps: list[tuple[float | None, str]] | None = None,
         description: str = "", decimals: int | None = None) -> dict:
    defaults = {"unit": unit, "thresholds": {"mode": "absolute", "steps": [
        {"value": v, "color": c} for v, c in (steps or [(None, "green")])]}}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {"type": "stat", "title": title, "description": description, "datasource": PROM,
            "targets": [target(expr, instant=False)], "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "background", "graphMode": "area", "textMode": "value"}}


def requests_by_job(window: str = "1m") -> list[dict]:
    return [
        target(f'sum by (job) (rate(http_server_requests_seconds_count{{{API}}}[{window}]))', "{{job}} HTTP", "A"),
        target(f'sum by (job) (rate(grpc_server_seconds_count[{window}]))', "{{job}} gRPC", "B"),
    ]


def overview() -> None:
    b = Board("overview", "Ledgerline: overview",
              "Are the APIs working, fast, and the dependencies answering? SLOs: 99.5% of requests without a server error, 99% within 1 s.")
    b.row("SLOs (the alerts page on the error budget's burn rate)")
    b.add(stat("API requests", "ledgerline:api_requests:rate5m", "reqps", description="the ledger's and payments' /v1, and the merchants' gRPC API; 5-minute rate"), 4, 4)
    b.add(stat("Failing (5 min)", "ledgerline:api_error_ratio:5m", "percentunit",
               [(None, "green"), (0.005, "orange"), (0.072, "red")], "server errors among requests; the SLO allows 0.5%, a page at 14.4x that", 2), 4, 4)
    b.add(stat("Over 1 s (5 min)", "ledgerline:api_slow_ratio:5m", "percentunit",
               [(None, "green"), (0.01, "orange"), (0.144, "red")], "requests slower than 1 s; the SLO allows 1%", 2), 4, 4)
    b.add(stat("Failing (1 h)", "ledgerline:api_error_ratio:1h", "percentunit",
               [(None, "green"), (0.005, "orange"), (0.072, "red")], decimals=3), 4, 4)
    b.add(stat("Alerts firing", 'count(ALERTS{alertstate="firing"}) or vector(0)', "short", [(None, "green"), (1, "red")]), 4, 4)
    b.add(stat("Ledger audit: problems", "sum(ledger_audit_violations)", "short", [(None, "green"), (1, "red")],
               "what the last audit of the books found wrong (it runs every 10 minutes)"), 4, 4)
    b.add({"type": "table", "title": "Firing and pending alerts", "datasource": PROM,
           "targets": [target('ALERTS', instant=True)],
           "transformations": [{"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True, "__name__": True, "instance": True},
                                                             "indexByName": {"alertname": 0, "alertstate": 1, "severity": 2}}}],
           "fieldConfig": {"defaults": {}, "overrides": []}, "options": {"showHeader": True}}, 24, 5)

    b.row("Traffic, latency, errors")
    b.add(series("Requests per second", requests_by_job(), "reqps", stack=True), 8, 8)
    b.add(series("Latency p99 (dots: sampled requests, click for the trace)", [
        target(f'histogram_quantile(0.99, sum by (le, job) (rate(http_server_requests_seconds_bucket{{{API}}}[5m])))', "{{job}} HTTP p99", "A", exemplar=True),
        target('histogram_quantile(0.99, sum by (le, job) (rate(grpc_server_seconds_bucket[5m])))', "{{job}} gRPC p99", "B", exemplar=True),
        target(f'histogram_quantile(0.5, sum by (le, job) (rate(http_server_requests_seconds_bucket{{{API}}}[5m])))', "{{job}} HTTP p50", "C"),
    ], "s", thresholds=[(1, "red")], description="the dashed line is the SLO's 1 s"), 8, 8)
    b.add(series("Server errors per second", [
        target(f'sum by (job, status) (rate(http_server_requests_seconds_count{{{API}, status=~"5.."}}[1m]))', "{{job}} HTTP {{status}}", "A"),
        target('sum by (job, grpc_status_code) (rate(grpc_server_seconds_count{grpc_status_code=~"UNAVAILABLE|INTERNAL|UNKNOWN|DEADLINE_EXCEEDED"}[1m]))',
               "{{job}} gRPC {{grpc_status_code}}", "B"),
    ], "reqps", stack=True), 8, 8)

    b.row("Who calls whom (from the traces)")
    b.add({"type": "nodeGraph", "title": "Service map", "datasource": TEMPO,
           "description": "Tempo's service graph: each edge is calls a second, how slow and how often failing (sampled traces).",
           "targets": [{"datasource": TEMPO, "queryType": "serviceMap", "refId": "A"}], "options": {}}, 12, 12)
    b.add(series("Dependencies: p99 as payments sees them", [
        target('histogram_quantile(0.99, sum by (le, peer_service) (rate(http_client_requests_seconds_bucket[5m])))', "{{peer_service}}", "A", exemplar=True),
    ], "s"), 12, 6)
    b.add(series("Dependencies: calls without an answer", [
        target('sum by (peer_service) (rate(http_client_requests_seconds_count{outcome!~"SUCCESS|CLIENT_ERROR"}[1m]))', "{{peer_service}}", "A"),
    ], "reqps", description="timeouts, refused connections and 5xx: each is retried with the same idempotency key"), 12, 6)
    b.write()


def payments() -> None:
    b = Board("payments", "Ledgerline: payments",
              "Is the money moving? A payment the ledger can't take now still gets an answer (202), so watch the work waiting here.")
    b.row("The work")
    b.add(series("State changes per second", [
        target('sum by (entity, to) (rate(payments_transitions_total[1m]))', "{{entity}} → {{to}}"),
    ], "ops", stack=True), 12, 8)
    b.add(series("Oldest waiting for its next step", [
        target("max by (entity) (payments_backlog_oldest_seconds)", "{{entity}}"),
    ], "s", thresholds=[(60, "red")], description="the alert: a payment or refund waiting over 60 s for a minute"), 6, 8)
    b.add(series("Waiting for a step", [target("sum by (entity) (payments_backlog_rows)", "{{entity}}")]), 6, 8)
    b.add(series("Retries and stuck steps", [
        target("sum by (entity) (rate(payments_retries_total[1m]))", "{{entity}} retried", "A"),
        target("sum by (entity) (increase(payments_stuck_total[5m]))", "{{entity}} stuck (5 min)", "B"),
    ], "short"), 8, 7)
    b.add(series("Outbox: how late events reach Kafka", [target("max by (job) (outbox_lag_seconds)", "{{job}}")], "s",
                 thresholds=[(30, "red")]), 8, 7)
    b.add(series("Outbox: events sent per second", [target("sum by (job) (rate(outbox_published_total[1m]))", "{{job}}")], "ops"), 8, 7)

    b.row("Merchants and fraud")
    b.add(series("Webhooks", [
        target("sum(rate(payments_webhooks_delivered_total[1m]))", "delivered", "A"),
        target("sum(rate(payments_webhooks_retried_total[1m]))", "refused, to retry", "B"),
        target("sum(rate(payments_webhooks_failed_total[1m]))", "given up (after 12 tries)", "C"),
    ], "ops"), 8, 7)
    b.add(series("Fraud: card payments unscored or declined", [
        target("sum(rate(payments_fraud_unscored_total[1m]))", "unscored (scorer down or slow: let through)", "A"),
        target("sum(rate(payments_fraud_declined_total[1m]))", "declined", "B"),
    ], "ops"), 8, 7)
    b.add(series("Fraud scorer: time to score", [
        target("histogram_quantile(0.99, sum by (le) (rate(fraud_score_seconds_bucket[5m])))", "p99", "A"),
        target('histogram_quantile(0.99, sum by (le) (rate(http_client_requests_seconds_bucket{peer_service="fraud"}[5m])))', "p99 as payments sees it (timeout 150 ms)", "B"),
    ], "s"), 8, 7)

    b.row("The books, and underneath")
    b.add(stat("Audit: problems found", "sum(ledger_audit_violations)", "short", [(None, "green"), (1, "red")]), 4, 5)
    b.add(stat("Audit: last completed", "time() - ledger_audit_last_success_seconds", "s", [(None, "green"), (1800, "red")]), 4, 5)
    b.add(stat("Audit: took", "ledger_audit_duration_seconds", "s"), 4, 5)
    b.add(series("Load shedding", [
        target("sum by (job) (admission_in_flight)", "{{job}} in flight", "A"),
        target("sum by (job) (rate(admission_shed_total[1m]))", "{{job}} turned away /s", "B"),
    ]), 12, 5)
    b.add(series("JVM heap used", [target('sum by (job) (jvm_memory_used_bytes{area="heap"})', "{{job}}")], "bytes"), 8, 7)
    b.add(series("Database connections in use", [
        target("sum by (job) (hikaricp_connections_active)", "{{job}} active", "A"),
        target("sum by (job) (hikaricp_connections_pending)", "{{job}} waiting", "B"),
    ]), 8, 7)
    b.add(series("GC pauses", [target("sum by (job) (rate(jvm_gc_pause_seconds_sum[1m]))", "{{job}}")], "percentunit",
                 description="share of time paused for garbage collection"), 8, 7)
    b.write()


if __name__ == "__main__":
    overview()
    payments()
    print("wrote", ", ".join(sorted(p.name for p in HERE.glob("*.json"))))
