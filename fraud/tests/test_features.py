"""python -m unittest discover -s tests"""

import unittest

from fraud.features import NAMES, Features, apply_event, epoch, rules
from fraud.train import replay


def ev(stream, version, kind, data):
    return {"stream": stream, "version": version, "type": kind, "data": data}


def authorising(pid, customer, merchant, amount, at):
    return ev(f"payment-{pid}", 1, "payment.authorising",
              {"id": pid, "customer": customer, "merchant": merchant, "amount": amount, "createdAt": at, "updatedAt": at})


def iso(t):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class FeaturesTest(unittest.TestCase):
    def test_windows_and_history_count_only_what_came_before(self):
        f = Features()
        for i in range(5):
            f.attempt(f"p{i}", "c", "m1", 10_000, 1000.0 + i * 30)
        x = dict(zip(NAMES, f.compute("c", "m2", 10_000, 1000.0 + 150)))
        self.assertEqual(x["n_1m"], 2)      # those at t=90 and t=120: a window includes its edge
        self.assertEqual(x["n_10m"], 5)
        self.assertEqual(x["new_merchant"], 1)
        self.assertEqual(x["merchant_share"], 0)
        self.assertAlmostEqual(x["amount_vs_max"], 1.0)

    def test_an_attempt_counts_once_whether_it_came_as_a_request_or_an_event(self):
        f = Features()
        self.assertTrue(f.attempt("p1", "c", "m", 500, 10.0))
        self.assertFalse(f.attempt("p1", "c", "m", 500, 10.0))
        apply_event(f, authorising("p1", "c", "m", 500, iso(10.0)))
        self.assertEqual(f.customers["c"].n, 1)

    def test_old_attempts_leave_the_windows(self):
        f = Features()
        f.attempt("p1", "c", "m", 500, 0.0)
        x = dict(zip(NAMES, f.compute("c", "m", 500, 4000.0)))
        self.assertEqual(x["n_1h"], 0)
        self.assertEqual(x["new_merchant"], 0)  # the customer's habits stay

    def test_a_card_test_trips_the_rules_and_a_habit_does_not(self):
        f = Features()
        for i in range(20):
            f.attempt(f"h{i}", "c", "fav", 50_000, i * 60.0)
        habit = f.compute("c", "fav", 52_000, 20 * 60.0)
        self.assertFalse(rules(habit))
        for i in range(5):
            f.attempt(f"t{i}", "c", f"x{i}", 300, 1300.0 + i * 2)
        self.assertTrue(rules(f.compute("c", "x9", 300, 1312.0)))

    def test_online_and_offline_give_the_same_features(self):
        events = [authorising(f"p{i}", "c", f"m{i % 3}", 1000 * (i + 1), iso(100.0 + i * 7)) for i in range(12)]
        # p3 is declined a second after it is made: its event arrives before p4's (7 s later).
        events.insert(4, ev("payment-p3", 3, "payment.declined", {"id": "p3", "customer": "c", "updatedAt": iso(100.0 + 3 * 7 + 1)}))
        offline = replay(events + events[:4])  # some delivered twice: skipped
        online = Features()
        for e in events:
            d = e["data"]
            if e["type"] == "payment.authorising":  # the scorer: features, then the request applied, then the event
                t = epoch(d["createdAt"])
                self.assertEqual(online.compute(d["customer"], d["merchant"], d["amount"], t), offline[d["id"]][1])
                online.attempt(d["id"], d["customer"], d["merchant"], d["amount"], t)
            apply_event(online, e)

    def test_java_timestamps_with_nanoseconds(self):
        self.assertAlmostEqual(epoch("2026-11-10T10:00:00.123456789Z") % 60, 0.123456, places=5)


if __name__ == "__main__":
    unittest.main()
