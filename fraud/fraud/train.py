"""Trains the model from a recorded run: the payment events from Kafka and the simulator's truth.

The events are replayed in time order through the same feature code the scorer runs (features.py): each
attempt's features are computed from what came before it. The split is by time, never at random (the
model is used on payments after the ones it learned from): the first 60% to train, the next 15% to pick
the threshold, the last 25% to measure. The threshold is the lowest score that declines at most 1% of
genuine attempts in the validation part.

The test part is scored by the model and by the rules (features.rules), with their thresholds both as an
analyst would first write them and tuned on the training part to the model's budget of genuine declines.
Writes model.txt, model.json and the report.

    python -m fraud.train --truth truth.jsonl --model-dir /model --report report.json   (env: KAFKA)
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict

import lightgbm as lgb
import numpy as np
from confluent_kafka import Consumer, TopicPartition
from sklearn.metrics import average_precision_score, roc_auc_score

from .features import NAMES, RULES, Features, epoch, rules

FPR_BUDGET = 0.01


def read_topic(topic: str) -> list[dict]:
    c = Consumer({"bootstrap.servers": os.environ["KAFKA"], "group.id": "fraud-train", "enable.auto.commit": False})
    meta = c.list_topics(topic, timeout=10)
    parts = [TopicPartition(topic, p, 0) for p in meta.topics[topic].partitions]
    c.assign(parts)
    ends = {p.partition: c.get_watermark_offsets(p, timeout=10)[1] for p in parts}
    pos = {p: 0 for p in ends}
    events = []
    while any(pos[p] < ends[p] for p in ends):
        for m in c.consume(5000, timeout=1):
            if m.error():
                continue
            pos[m.partition()] = m.offset() + 1
            events.append(json.loads(m.value()))
    c.close()
    return events


def replay(events: list[dict]) -> dict[str, tuple[float, list[float]]]:
    """Every attempt's features as the scorer would have computed them: payment id -> (time, features)."""
    timeline = []
    seen = set()
    for e in events:
        if not e["type"].startswith("payment."):
            continue
        d = e["data"]
        key = (e["stream"], e["version"])
        if key in seen:  # at least once: a consumer skips what it has seen
            continue
        seen.add(key)
        if e["type"] == "payment.authorising":
            timeline.append((epoch(d["createdAt"]), 0, d))
        elif e["type"] == "payment.declined":
            timeline.append((epoch(d["updatedAt"]), 1, d))
    timeline.sort(key=lambda x: (x[0], x[1]))
    f = Features()
    out = {}
    for t, kind, d in timeline:
        if kind == 0:
            out[d["id"]] = (t, f.compute(d["customer"], d["merchant"], int(d["amount"]), t))
            f.attempt(d["id"], d["customer"], d["merchant"], int(d["amount"]), t)
        else:
            f.decline(d["id"], d["customer"], t)
    return out


def measure(y: np.ndarray, flagged: np.ndarray, amounts: np.ndarray, patterns: list[str]) -> dict:
    fraud, legit = y == 1, y == 0
    per_pattern = {}
    for p in sorted(set(patterns)):
        idx = np.array([q == p for q in patterns])
        per_pattern[p] = {"attempts": int(idx.sum()), "flagged": round(float(flagged[idx].mean()), 4)}
    return {
        "fraud_caught": round(float(flagged[fraud].mean()), 4),
        "fraud_money_caught": round(float(amounts[fraud & flagged].sum() / max(1, amounts[fraud].sum())), 4),
        "genuine_declined": round(float(flagged[legit].mean()), 4),
        "genuine_money_declined": round(float(amounts[legit & flagged].sum() / max(1, amounts[legit].sum())), 4),
        "precision": round(float(y[flagged].mean()), 4) if flagged.any() else None,
        "by_pattern": per_pattern,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--truth", nargs="+", required=True)
    ap.add_argument("--model-dir", required=True)
    ap.add_argument("--report", required=True)
    ap.add_argument("--topic", default="payments.events")
    args = ap.parse_args()

    truth = {}
    for path in args.truth:
        for line in open(path):
            row = json.loads(line)
            if row.get("payment"):
                truth[row["payment"]] = row
    feats = replay(read_topic(args.topic))
    ids = sorted((pid for pid in truth if pid in feats), key=lambda pid: feats[pid][0])
    X = np.array([feats[p][1] for p in ids])
    y = np.array([1 if truth[p]["fraud"] else 0 for p in ids])
    amounts = np.array([truth[p]["amount"] for p in ids], dtype=float)
    patterns = [truth[p]["pattern"] for p in ids]
    n = len(ids)
    a, b = int(n * 0.60), int(n * 0.75)

    params = {
        "objective": "binary", "learning_rate": 0.05, "num_leaves": 31, "min_data_in_leaf": 20,
        "feature_fraction": 0.9, "bagging_fraction": 0.8, "bagging_freq": 1, "verbose": -1, "seed": 7,
        "num_threads": 1,
    }
    train, valid = lgb.Dataset(X[:a], y[:a], feature_name=NAMES), lgb.Dataset(X[a:b], y[a:b], feature_name=NAMES)
    booster = lgb.train(params, train, num_boost_round=600, valid_sets=[valid],
                        callbacks=[lgb.early_stopping(50, verbose=False)])

    # The threshold: decline at most 1% of genuine attempts in validation.
    sv = booster.predict(X[a:b])
    genuine = np.sort(sv[y[a:b] == 0])
    threshold = float(genuine[int(np.ceil(len(genuine) * (1 - FPR_BUDGET))) - 1]) + 1e-6 if len(genuine) else 0.5
    threshold = max(threshold, 0.05)

    # The rules, tuned as fairly: the thresholds that catch the most fraud in the training part while
    # declining at most 1% of its genuine attempts.
    best, best_recall = dict(RULES), -1.0
    tr_fraud, tr_genuine = y[:b] == 1, y[:b] == 0
    col = {k: X[:b, NAMES.index(k)] for k in ("n_1m", "small_10m", "merchants_10m", "new_merchant", "amount_z", "history_log")}
    off = 1e9  # a condition switched off
    for n1 in (3, 4, 5, 6, 8, off):
        for small in (2, 3, 4, 6, off):
            for merchants in (5, 8, 10, 12, 15, 20, off):
                for z in (2.0, 2.5, 3.0, 4.0, 5.0, off):
                    flags = ((col["n_1m"] >= n1) | (col["small_10m"] >= small) | (col["merchants_10m"] >= merchants)
                             | ((col["new_merchant"] == 1) & (col["amount_z"] > z) & (col["history_log"] >= np.log1p(5))))
                    if flags[tr_genuine].mean() <= FPR_BUDGET and flags[tr_fraud].mean() > best_recall:
                        best_recall = float(flags[tr_fraud].mean())
                        best = {"n_1m": n1, "small_10m": small, "merchants_10m": merchants, "amount_z": z}

    st = booster.predict(X[b:])
    yt, at, pt = y[b:], amounts[b:], patterns[b:]
    model_flags = st >= threshold
    rule_flags = np.array([rules(list(x), best) for x in X[b:]])
    untuned_flags = np.array([rules(list(x)) for x in X[b:]])
    importance = dict(zip(NAMES, booster.feature_importance("gain").round(1).tolist()))
    report = {
        "attempts": n, "fraud_share": round(float(y.mean()), 4),
        "split": {"train": a, "validation": b - a, "test": n - b},
        "test": {"attempts": n - b, "fraud": int(yt.sum())},
        "model": {
            "rounds": booster.best_iteration, "threshold": round(threshold, 4),
            "pr_auc": round(float(average_precision_score(yt, st)), 4),
            "roc_auc": round(float(roc_auc_score(yt, st)), 4),
            **measure(yt, model_flags, at, pt),
        },
        "rules_tuned": {"thresholds": best, **measure(yt, rule_flags, at, pt)},
        "rules_untuned": {"thresholds": RULES, **measure(yt, untuned_flags, at, pt)},
        "importance_by_gain": dict(sorted(importance.items(), key=lambda kv: -kv[1])),
    }
    os.makedirs(args.model_dir, exist_ok=True)
    booster.save_model(os.path.join(args.model_dir, "model.txt"), num_iteration=booster.best_iteration)
    with open(os.path.join(args.model_dir, "model.json"), "w") as fh:
        json.dump({"name": "lightgbm-v1", "features": NAMES, "threshold": threshold, "trained_on": a, "rules": best}, fh, indent=1)
    with open(args.report, "w") as fh:
        json.dump(report, fh, indent=1)
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
