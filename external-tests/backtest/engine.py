"""Deterministic long/flat simulation. This module knows no storage backend."""

import hashlib
import json

import numpy as np

CASES = ("rsi", "sma", "momentum")
PARAMETERS = {
    "rsi": {"period": 14, "buy_at_or_below": 30, "sell_at_or_above": 70},
    "sma": {"fast": 20, "slow": 60},
    "momentum": {"lookback": 60, "rule": "close > close_60_available_bars_ago"},
}


class Signal:
    def __init__(self, case):
        if case not in CASES:
            raise ValueError(case)
        self.case = case
        self.tail = np.empty(0, dtype=np.int64)
        self.previous = None
        self.changes = 0
        self.gain = self.loss = 0.0
        self.target = False

    def consume(self, close):
        if self.case == "rsi":
            return self.rsi(close)
        joined = np.concatenate((self.tail, close))
        offset = len(self.tail)
        desired = np.zeros(len(joined), dtype=bool)
        if self.case == "momentum":
            desired[60:] = joined[60:] > joined[:-60]
        else:
            # Integer rolling sums give identical results regardless of batches.
            if len(joined) and int(np.max(joined)) * len(joined) > np.iinfo(np.int64).max:
                raise ValueError("SMA cumulative sum would overflow int64")
            sums = np.concatenate((np.zeros(1, dtype=np.int64), np.cumsum(joined)))
            if len(joined) >= 60:
                fast = sums[60:] - sums[40:-20]
                slow = sums[60:] - sums[:-60]
                # The products are bounded independently of the cumulative sum.
                if int(np.max(joined)) > np.iinfo(np.int64).max // (20 * 60):
                    raise ValueError("SMA comparison would overflow int64")
                desired[59:] = fast * 60 > slow * 20
        self.tail = joined[-60:].copy()
        return desired[offset:]

    def rsi(self, close):
        desired = np.empty(len(close), dtype=bool)
        for i, value in enumerate(close):
            value = int(value)
            if self.previous is not None:
                delta = value - self.previous
                gain, loss = float(max(delta, 0)), float(max(-delta, 0))
                self.changes += 1
                if self.changes <= 14:
                    self.gain += gain
                    self.loss += loss
                    if self.changes == 14:
                        self.gain /= 14
                        self.loss /= 14
                else:
                    self.gain = (self.gain * 13 + gain) / 14
                    self.loss = (self.loss * 13 + loss) / 14
                if self.changes >= 14:
                    total = self.gain + self.loss
                    rsi = 100 * self.gain / total if total else 50.0
                    if rsi <= 30:
                        self.target = True
                    elif rsi >= 70:
                        self.target = False
            self.previous = value
            desired[i] = self.target
        return desired


class Account:
    def __init__(self, symbol, case, cash, scale, fee, slippage):
        self.symbol = symbol
        self.signal = Signal(case)
        self.cash = cash
        self.units = 0.0
        self.pending = False
        self.last_ts = None
        self.last_close = None
        self.scale = scale
        self.fee = fee
        self.slippage = slippage
        self.trades = []

    def consume(self, bars):
        n = len(bars)
        if not n:
            return np.empty(0)
        if np.any(bars["ts"][1:] <= bars["ts"][:-1]) or (
            self.last_ts is not None and int(bars["ts"][0]) <= self.last_ts
        ):
            raise ValueError("overlapping or unordered bars")
        if np.any(bars["open"] <= 0) or np.any(bars["close"] <= 0):
            raise ValueError("execution requires positive prices")
        desired = self.signal.consume(bars["close"])
        positions = np.empty(n, dtype=bool)
        positions[0], positions[1:] = self.pending, desired[:-1]
        changes = np.empty(n, dtype=bool)
        changes[0] = positions[0] != bool(self.units)
        changes[1:] = positions[1:] != positions[:-1]
        events = np.flatnonzero(changes)
        equity = np.empty(n, dtype=np.float64)
        close = bars["close"].astype(np.float64) / self.scale
        begin = 0
        for index in events:
            equity[begin:index] = self.cash + self.units * close[begin:index]
            self.fill(
                int(bars["ts"][index]), int(bars["open"][index]) / self.scale, positions[index]
            )
            begin = index
        equity[begin:] = self.cash + self.units * close[begin:]
        self.pending = bool(desired[-1])
        self.last_ts = int(bars["ts"][-1])
        self.last_close = float(close[-1])
        return equity

    def fill(self, ts, price, buy):
        if buy:
            price *= 1 + self.slippage
            self.units = self.cash / (price * (1 + self.fee))
            fee = self.units * price * self.fee
            self.cash = 0.0
            quantity = self.units
        else:
            price *= 1 - self.slippage
            quantity = self.units
            fee = self.units * price * self.fee
            self.cash = self.units * price - fee
            self.units = 0.0
        self.trades.append(
            {
                "symbol": self.symbol,
                "ts": ts,
                "side": "buy" if buy else "sell",
                "price": price,
                "quantity": quantity,
                "quantity_after": self.units,
                "fee": fee,
                "cash_after": self.cash,
            }
        )


class Backtest:
    def __init__(self, case, symbols, scale, *, capital=100_000.0, fee_bps=10.0, slippage_bps=1.0):
        if (
            not symbols
            or scale <= 0
            or capital <= 0
            or fee_bps < 0
            or not 0 <= slippage_bps < 10_000
        ):
            raise ValueError("invalid portfolio parameters")
        self.symbols = sorted(symbols)
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("duplicate symbols")
        self.case = case
        self.capital = capital
        self.accounts = {
            s: Account(
                s, case, capital / len(symbols), scale, fee_bps / 10_000, slippage_bps / 10_000
            )
            for s in self.symbols
        }
        self.peak = capital
        self.drawdown = 0.0
        self.curve = hashlib.sha256()
        self.rows = 0

    def consume(self, frames):
        if set(frames) != set(self.symbols):
            raise ValueError("every batch must contain each portfolio symbol")
        ts = frames[self.symbols[0]]["ts"]
        total = np.zeros(len(ts), dtype=np.float64)
        for symbol in self.symbols:
            if not np.array_equal(ts, frames[symbol]["ts"]):
                raise ValueError("portfolio requires aligned timestamps; no silent forward fill")
            total += self.accounts[symbol].consume(frames[symbol])
        if len(total):
            highs = np.maximum(np.maximum.accumulate(total), self.peak)
            self.drawdown = max(self.drawdown, float(np.max(1 - total / highs)))
            self.peak = float(highs[-1])
            self.curve.update(total.astype("<f8", copy=False).tobytes())
            self.rows += len(ts) * len(self.symbols)

    def result(self):
        trades = sorted(
            (t for a in self.accounts.values() for t in a.trades),
            key=lambda t: (t["ts"], t["symbol"]),
        )
        equity = sum(a.cash + a.units * (a.last_close or 0) for a in self.accounts.values())
        return {
            "case": self.case,
            "rows": self.rows,
            "final_equity": equity,
            "return_fraction": equity / self.capital - 1,
            "max_drawdown": self.drawdown,
            "fills": len(trades),
            "completed_round_trips": sum(t["side"] == "sell" for t in trades),
            "fees": sum(t["fee"] for t in trades),
            "open_positions": {s: a.units for s, a in self.accounts.items()},
            "pending_targets": {s: a.pending for s, a in self.accounts.items()},
            "equity_sha256": self.curve.hexdigest(),
            "trades_sha256": hashlib.sha256(
                json.dumps(trades, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            ).hexdigest(),
        }
