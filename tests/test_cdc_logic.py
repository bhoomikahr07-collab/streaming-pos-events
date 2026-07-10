"""
Unit tests for the CDC resolution and windowing logic. Pure stdlib
unittest, no Spark needed — this is the whole point of keeping
src/cdc_logic.py Spark-free.

Usage:
    python -m unittest discover tests -v
"""
import os
import sys
import unittest
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from cdc_logic import resolve_latest_state, window_start, compute_windowed_aggregates


class TestResolveLatestState(unittest.TestCase):
    def test_single_insert_passes_through(self):
        events = [{"transaction_id": "T1", "op": "INSERT", "op_seq": 1, "quantity": 2}]
        result = resolve_latest_state(events)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["quantity"], 2)

    def test_update_overrides_insert(self):
        events = [
            {"transaction_id": "T1", "op": "INSERT", "op_seq": 1, "quantity": 2},
            {"transaction_id": "T1", "op": "UPDATE", "op_seq": 2, "quantity": 5},
        ]
        result = resolve_latest_state(events)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["quantity"], 5)
        self.assertEqual(result[0]["op"], "UPDATE")

    def test_delete_excludes_transaction_entirely(self):
        events = [
            {"transaction_id": "T1", "op": "INSERT", "op_seq": 1, "quantity": 2},
            {"transaction_id": "T1", "op": "DELETE", "op_seq": 2, "quantity": 2},
        ]
        result = resolve_latest_state(events)
        self.assertEqual(len(result), 0)

    def test_out_of_order_arrival_still_resolves_to_highest_op_seq(self):
        events = [
            {"transaction_id": "T1", "op": "UPDATE", "op_seq": 2, "quantity": 5},
            {"transaction_id": "T1", "op": "INSERT", "op_seq": 1, "quantity": 2},
        ]
        result = resolve_latest_state(events)
        self.assertEqual(result[0]["quantity"], 5)

    def test_multiple_independent_transactions(self):
        events = [
            {"transaction_id": "T1", "op": "INSERT", "op_seq": 1, "quantity": 2},
            {"transaction_id": "T2", "op": "INSERT", "op_seq": 1, "quantity": 3},
            {"transaction_id": "T2", "op": "DELETE", "op_seq": 2, "quantity": 3},
        ]
        result = resolve_latest_state(events)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["transaction_id"], "T1")


class TestWindowStart(unittest.TestCase):
    def test_floors_to_five_minute_boundary(self):
        ts = datetime(2026, 6, 1, 9, 7, 42)
        result = window_start(ts, window_minutes=5)
        self.assertEqual(result, datetime(2026, 6, 1, 9, 5, 0))

    def test_exact_boundary_stays_put(self):
        ts = datetime(2026, 6, 1, 9, 10, 0)
        result = window_start(ts, window_minutes=5)
        self.assertEqual(result, datetime(2026, 6, 1, 9, 10, 0))


class TestComputeWindowedAggregates(unittest.TestCase):
    def test_aggregates_within_same_window_and_store(self):
        events = [
            {"event_ts": "2026-06-01T09:01:00", "store_id": "S1", "quantity": 2, "unit_price": 10.0},
            {"event_ts": "2026-06-01T09:03:00", "store_id": "S1", "quantity": 1, "unit_price": 5.0},
        ]
        result = compute_windowed_aggregates(events, window_minutes=5)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["total_revenue"], 25.0)
        self.assertEqual(result[0]["total_units"], 3)
        self.assertEqual(result[0]["transaction_count"], 2)

    def test_separates_different_stores_same_window(self):
        events = [
            {"event_ts": "2026-06-01T09:01:00", "store_id": "S1", "quantity": 1, "unit_price": 10.0},
            {"event_ts": "2026-06-01T09:01:00", "store_id": "S2", "quantity": 1, "unit_price": 10.0},
        ]
        result = compute_windowed_aggregates(events, window_minutes=5)
        self.assertEqual(len(result), 2)

    def test_separates_different_windows_same_store(self):
        events = [
            {"event_ts": "2026-06-01T09:01:00", "store_id": "S1", "quantity": 1, "unit_price": 10.0},
            {"event_ts": "2026-06-01T09:11:00", "store_id": "S1", "quantity": 1, "unit_price": 10.0},
        ]
        result = compute_windowed_aggregates(events, window_minutes=5)
        self.assertEqual(len(result), 2)

    def test_end_to_end_correction_reflected_in_aggregate(self):
        events = [
            {"transaction_id": "T1", "op": "INSERT", "op_seq": 1,
             "event_ts": "2026-06-01T09:01:00", "store_id": "S1", "quantity": 2, "unit_price": 10.0},
            {"transaction_id": "T1", "op": "UPDATE", "op_seq": 2,
             "event_ts": "2026-06-01T09:01:30", "store_id": "S1", "quantity": 5, "unit_price": 10.0},
        ]
        resolved = resolve_latest_state(events)
        result = compute_windowed_aggregates(resolved, window_minutes=5)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["total_revenue"], 50.0)
        self.assertEqual(result[0]["transaction_count"], 1)


if __name__ == "__main__":
    unittest.main()
