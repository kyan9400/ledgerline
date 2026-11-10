"""Online against offline: replays a live run's events through the same features and model, and compares
each payment's score with the one the scorer gave it live. Any difference is skew between serving and
training.

    python -m fraud.parity truth.jsonl     (env: KAFKA, MODEL_DIR)
"""

from __future__ import annotations

import json
import os
import sys

import lightgbm as lgb
import numpy as np

from .train import read_topic, replay


def main() -> None:
    truth = [json.loads(line) for line in open(sys.argv[1])]
    truth = [r for r in truth if r.get("payment") and r.get("score") is not None]
    model_dir = os.environ.get("MODEL_DIR", "/model")
    booster = lgb.Booster(model_file=os.path.join(model_dir, "model.txt"))
    threshold = json.load(open(os.path.join(model_dir, "model.json")))["threshold"]
    feats = replay(read_topic("payments.events"))
    rows = [r for r in truth if r["payment"] in feats]
    offline = booster.predict(np.array([feats[r["payment"]][1] for r in rows]))
    online = np.array([r["score"] for r in rows])
    genuine = np.array([not r["fraud"] for r in rows])
    diff = np.abs(offline - online)
    print(json.dumps({
        "payments": len(rows),
        "same_score_within_0.001": round(float((diff < 1e-3).mean()), 4),
        "same_decision": round(float(((offline >= threshold) == (online >= threshold)).mean()), 4),
        "max_difference": round(float(diff.max()), 4),
        "genuine_declined_online": round(float((online[genuine] >= threshold).mean()), 4),
        "genuine_declined_offline": round(float((offline[genuine] >= threshold).mean()), 4),
    }, indent=1))


if __name__ == "__main__":
    main()
