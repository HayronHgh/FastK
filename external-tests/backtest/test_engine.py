"""Causality, month boundaries, fees and common adapter failure semantics."""

import unittest
from unittest.mock import patch

import numpy as np
from fastk_bench.batches import dtype
from fastk_bench.data import FIELDS

from .engine import Account, Backtest, Signal
from .worker import read_batch


def bars(close, opens=None, start=0):
    result = np.empty(len(close), dtype=dtype(FIELDS))
    result["ts"] = np.arange(start, start + len(close)) * 60_000
    for field in ("open", "high", "low", "close"):
        result[field] = close
    if opens is not None:
        result["open"] = opens
    result["volume"] = 1
    result.flags.writeable = False
    return result


class EngineTests(unittest.TestCase):
    def test_rsi_seed_neutral_and_next_open_not_signal_close(self):
        flat = Signal("rsi")
        self.assertFalse(flat.consume(np.full(100, 100)).any())
        data = bars(list(range(100, 84, -1)), [100] * 15 + [200])
        engine = Backtest("rsi", ["A"], 1, capital=1000, fee_bps=0, slippage_bps=0)
        engine.consume({"A": data[:15]})
        self.assertEqual(engine.accounts["A"].trades, [])
        self.assertTrue(engine.accounts["A"].pending)
        engine.consume({"A": data[15:]})
        trade = engine.accounts["A"].trades[0]
        self.assertEqual((trade["ts"], trade["price"], trade["quantity"]), (900_000, 200, 5))
        self.assertEqual(engine.result()["final_equity"], 425)
        # Final-close signal is pending only; no invented terminal execution.
        self.assertEqual(engine.result()["fills"], 1)

    def test_actual_costs_charge_both_sides_and_no_terminal_liquidation(self):
        account = Account("A", "rsi", 1000, 1, 0.001, 0.0001)
        account.fill(1, 100, True)
        expected_units = 1000 / (100.01 * 1.001)
        self.assertAlmostEqual(account.units, expected_units)
        account.fill(2, 110, False)
        self.assertAlmostEqual(account.cash, expected_units * 109.989 * 0.999)
        self.assertEqual(account.trades[1]["quantity"], expected_units)
        self.assertEqual(account.units, 0)

    def test_vector_signals_against_independent_scalar_windows(self):
        values = np.random.default_rng(7).integers(80, 120, 500, dtype=np.int64)
        for case in ("sma", "momentum"):
            signal = Signal(case)
            actual = np.concatenate([signal.consume(c) for c in np.array_split(values, 17)])
            expected = []
            for i, value in enumerate(values):
                if case == "sma":
                    expected.append(
                        i >= 59
                        and sum(map(int, values[i - 19 : i + 1])) * 60
                        > sum(map(int, values[i - 59 : i + 1])) * 20
                    )
                else:
                    expected.append(i >= 60 and value > values[i - 60])
            np.testing.assert_array_equal(actual, expected)

    def test_entire_curve_and_fills_invariant_under_month_splits_with_gaps(self):
        rng = np.random.default_rng(10)
        data = bars(10000 + np.cumsum(rng.integers(-20, 21, 3000)))
        data = data.copy()
        data["ts"][1500:] += 86_400_000  # preserve missing time, carry position
        data.flags.writeable = False
        for case in ("rsi", "sma", "momentum"):
            full = Backtest(case, ["A", "B"], 1)
            full.consume({"A": data, "B": data})
            monthly = Backtest(case, ["A", "B"], 1)
            for piece in np.array_split(data, 31):
                monthly.consume({"A": piece, "B": piece})
            self.assertGreater(full.result()["fills"], 0)
            self.assertEqual(full.result(), monthly.result())
            self.assertEqual(full.accounts["A"].trades, monthly.accounts["A"].trades)

    def test_momentum_first_execution_at_61_and_chunk_pending_survives(self):
        data = bars(np.arange(100, 164))
        e = Backtest("momentum", ["A"], 1, capital=1000, fee_bps=0, slippage_bps=0)
        e.consume({"A": data[:61]})
        self.assertEqual(e.result()["fills"], 0)
        e.consume({"A": data[61:]})
        self.assertEqual(e.accounts["A"].trades[0]["ts"], 61 * 60_000)
        self.assertAlmostEqual(e.result()["final_equity"], 1000 / 161 * 163)

    def test_reject_overlap_and_unaligned_portfolio(self):
        data = bars([100] * 100)
        e = Backtest("sma", ["A"], 1)
        e.consume({"A": data})
        with self.assertRaisesRegex(ValueError, "overlapping"):
            e.consume({"A": data})
        with self.assertRaisesRegex(ValueError, "aligned"):
            Backtest("sma", ["A", "B"], 1).consume({"A": data, "B": data[1:]})

    def test_failure_closes_once_and_does_not_replay(self):
        class Failing:
            queries = 0
            closes = 0

            def query(self, q):
                self.queries += 1
                raise RuntimeError("read failed")

            def close(self):
                self.closes += 1

        backend = Failing()
        with patch("backtest.worker.backends.connect", return_value=backend):
            with self.assertRaisesRegex(RuntimeError, "read failed"):
                read_batch("fastk", "unused", {}, [{"symbol": "A"}])
        self.assertEqual((backend.queries, backend.closes), (1, 1))


if __name__ == "__main__":
    unittest.main()
