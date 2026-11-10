"""The fraud scorer: features from Kafka, a LightGBM model, and an HTTP endpoint the payments service asks
before it moves a card payment's money.

    POST /v1/score   {payment, customer, merchant, amount, at}
                     -> {score, decision: approve | decline, threshold, model, reasons}

- **State from Kafka.** On start it reads `payments.events` from the beginning and rebuilds every
  customer's and merchant's features, then follows the topic. It is ready only when it has caught up
  with what was there when it started, and until then answers 503 (the payments service then lets the
  payment through, unscored: it fails open).
- **The request counts too.** A scored payment is applied to the state at once, by its id, so a burst
  of attempts doesn't wait for the topic; its event, when it arrives, is skipped as already seen.
- **The model** is `model.txt` with `model.json` beside it (feature names, threshold), from train.py.
  Without one, the rules in features.py decide.

Environment: KAFKA (bootstrap servers), TOPIC=payments.events, MODEL_DIR=/model, THRESHOLD (overrides).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path

import lightgbm as lgb
import numpy as np
from confluent_kafka import Consumer, TopicPartition
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel

from .features import NAMES, Features, apply_event, epoch, rules

log = logging.getLogger("fraud")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SCORES = Histogram("fraud_score", "scores given", buckets=[0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9, 0.99])
DECISIONS = Counter("fraud_decisions_total", "decisions", ["decision"])
EVENTS = Counter("fraud_events_applied_total", "payment events applied")
READY = Gauge("fraud_ready", "caught up with the topic")
SCORE_TIME = Histogram("fraud_score_seconds", "time to score", buckets=[0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05])


class Model:
    def __init__(self, directory: Path):
        meta_file = directory / "model.json"
        self.booster = None
        self.threshold = float(os.environ.get("THRESHOLD", "0.5"))
        self.name = "rules"
        if meta_file.exists():
            meta = json.loads(meta_file.read_text())
            if meta["features"] != NAMES:
                raise SystemExit(f"model features {meta['features']} are not this code's {NAMES}")
            self.booster = lgb.Booster(model_file=str(directory / "model.txt"))
            self.threshold = float(os.environ.get("THRESHOLD", meta["threshold"]))
            self.name = meta["name"]
        log.info("model %s, threshold %.3f", self.name, self.threshold)

    def score(self, f: list[float]) -> tuple[float, list[str]]:
        if self.booster is None:
            return (1.0 if rules(f) else 0.0), []
        x = np.array([f])
        score = float(self.booster.predict(x)[0])
        contrib = self.booster.predict(x, pred_contrib=True)[0][:-1]  # per feature; the last is the bias
        top = [NAMES[i] for i in np.argsort(-contrib)[:3] if contrib[i] > 0]
        return score, top


features = Features()
model = Model(Path(os.environ.get("MODEL_DIR", "/model")))
ready = threading.Event()


def consume() -> None:
    """Rebuilds the state from the start of the topic, then follows it."""
    topic = os.environ.get("TOPIC", "payments.events")
    while True:
        try:
            consumer = Consumer({
                "bootstrap.servers": os.environ["KAFKA"],
                "group.id": "fraud-features",  # offsets are never committed: every start rebuilds from the start
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            })
            meta = consumer.list_topics(topic, timeout=10)
            if topic not in meta.topics or meta.topics[topic].error is not None:
                raise RuntimeError(f"no topic {topic} yet")
            parts = [TopicPartition(topic, p, 0) for p in meta.topics[topic].partitions]
            consumer.assign(parts)
            ends = {p.partition: consumer.get_watermark_offsets(p, timeout=10)[1] for p in parts}
            log.info("rebuilding from %s: %d events", topic, sum(ends.values()))
            started = time.time()
            positions = {p: 0 for p in ends}
            while True:
                for msg in consumer.consume(500, timeout=0.5):
                    if msg.error():
                        continue
                    positions[msg.partition()] = msg.offset() + 1
                    apply_event(features, json.loads(msg.value()))
                    EVENTS.inc()
                if not ready.is_set() and all(positions[p] >= ends[p] for p in ends):
                    log.info("caught up in %.1f s", time.time() - started)
                    ready.set()
                    READY.set(1)
        except Exception as e:  # Kafka not there yet, or gone: try again
            log.warning("consumer: %s", e)
            time.sleep(2)


app = FastAPI(title="Ledgerline fraud scorer")


@app.on_event("startup")
def start() -> None:
    threading.Thread(target=consume, daemon=True, name="consumer").start()


class Attempt(BaseModel):
    payment: str
    customer: str
    merchant: str
    amount: int
    at: str  # ISO-8601: when the payment was created


@app.post("/v1/score")
def score(a: Attempt, response: Response):
    if not ready.is_set():
        response.status_code = 503
        return {"code": "not_ready", "detail": "still rebuilding the features from Kafka"}
    started = time.perf_counter()
    t = epoch(a.at)
    f = features.compute(a.customer, a.merchant, a.amount, t)
    s, reasons = model.score(f)
    features.attempt(a.payment, a.customer, a.merchant, a.amount, t)
    decision = "decline" if s >= model.threshold else "approve"
    SCORE_TIME.observe(time.perf_counter() - started)
    SCORES.observe(s)
    DECISIONS.labels(decision).inc()
    return {"score": round(s, 4), "decision": decision, "threshold": model.threshold, "model": model.name, "reasons": reasons}


@app.get("/health")
def health(response: Response):
    if not ready.is_set():
        response.status_code = 503
    return {"ready": ready.is_set(), "customers": len(features.customers), "model": model.name}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
