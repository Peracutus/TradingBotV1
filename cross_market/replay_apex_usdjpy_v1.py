#!/usr/bin/env python3
"""Independent stdlib replay of Apex Breakout USDJPY H1 Session v1.

Rules and defaults are transcribed from pinned upstream research/validate.py,
research/strategies.py, research/engine.py and the shipped .set preset. This
script reads our own HistData BID H1 aggregation; it never loads upstream code.
The author's H1 backtest exits on the *close* of the 19:00 UTC bar. The EA
attempts to close on the first tick at or after 20:00 UTC. A second variant
exits at the 19:00 open only as an earlier-exit sensitivity check.
"""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "histdata_2021_2026" / "USDJPY_60_bid.csv"
OUT = ROOT / "apex_usdjpy_v1_independent_replay_2026-10-13.json"
UPSTREAM_COMMIT = "3dab20e20a846edae9ac6fcf56d6b090dbba9f98"


@dataclass
class Position:
    side: int
    entry: float
    stop: float
    units: float
    entry_i: int


def load() -> list[dict]:
    with DATA.open(newline="") as stream:
        rows = [{"time": int(x["time"]), **{k: float(x[k]) for k in ("open", "high", "low", "close")}}
                for x in csv.DictReader(stream)]
    if len(rows) < 1000 or len({r["time"] for r in rows}) != len(rows):
        raise ValueError("Incomplete or duplicate USDJPY H1 data")
    return rows


def atr_wilder(rows: list[dict], n: int = 14) -> list[float | None]:
    tr = []
    for i, row in enumerate(rows):
        prev = rows[i-1]["close"] if i else row["close"]
        tr.append(max(row["high"]-row["low"], abs(row["high"]-prev), abs(row["low"]-prev)))
    values: list[float | None] = [None] * len(rows)
    values[n] = sum(tr[1:n+1])/n
    for i in range(n+1, len(rows)):
        values[i] = values[i-1]*(n-1)/n + tr[i]/n
    return values


def signal_and_time_exit(rows: list[dict], atr: list[float | None]) -> tuple[list[int], list[bool]]:
    signals = [0] * len(rows)
    exits = [False] * len(rows)
    range_hi = range_lo = None
    taken_long = taken_short = False
    for i in range(1, len(rows)):
        hour = datetime.fromtimestamp(rows[i]["time"]/1000, timezone.utc).hour
        prev_hour = datetime.fromtimestamp(rows[i-1]["time"]/1000, timezone.utc).hour
        if hour < prev_hour:
            range_hi = range_lo = None
            taken_long = taken_short = False
        if 0 <= hour < 7:
            range_hi = rows[i]["high"] if range_hi is None else max(range_hi, rows[i]["high"])
            range_lo = rows[i]["low"] if range_lo is None else min(range_lo, rows[i]["low"])
        elif 7 <= hour < 12 and range_hi is not None:
            buffer = 0.25 * (atr[i] or 0.0)
            if not taken_long and rows[i]["close"] > range_hi + buffer:
                signals[i] = 1
                taken_long = True
            if not taken_short and rows[i]["close"] < range_lo - buffer:
                signals[i] = -1
                taken_short = True
        exits[i] = hour >= 19
    return signals, exits


def replay(rows: list[dict], atr: list[float | None], signals: list[int],
           exits: list[bool], *, start: str, end: str, risk_pct: float,
           leverage_cap: float, cost_jpy: float, force_close_at_19_open: bool = False) -> dict:
    dates = [datetime.fromtimestamp(r["time"]/1000, timezone.utc).date().isoformat() for r in rows]
    lo = next(i for i, d in enumerate(dates) if d >= start)
    hi = next((i for i, d in enumerate(dates) if d >= end), len(rows))
    balance = 1000.0
    position = None
    ledger = []
    curve = []
    peak = balance
    max_dd = 0.0

    def pnl(p: Position, exit_price: float, conversion: float) -> float:
        return p.units * ((exit_price-p.entry)*p.side-cost_jpy)/conversion

    def close(i: int, price: float, reason: str) -> None:
        nonlocal balance, position
        p = position
        assert p is not None
        earned = pnl(p, price, rows[i]["close"])
        balance += earned
        ledger.append({"side": p.side, "entry_ms": rows[p.entry_i]["time"],
                       "exit_ms": rows[i]["time"], "entry": p.entry,
                       "exit": price, "units": p.units, "pnl_usd": earned,
                       "reason": reason})
        position = None

    for i in range(lo, hi):
        row = rows[i]
        hour = datetime.fromtimestamp(row["time"]/1000, timezone.utc).hour
        if position is not None and force_close_at_19_open and hour >= 19:
            close(i, row["open"], "19_utc_open")
        if position is None and signals[i-1] != 0 and atr[i-1] is not None:
            side = signals[i-1]
            dist = 2 * atr[i-1]
            units = min(balance * risk_pct/100 * row["close"]/dist,
                        balance * leverage_cap)
            if units > 0:
                position = Position(side, row["open"], row["open"]-side*dist, units, i)
        if position is not None:
            p = position
            hit = row["low"] <= p.stop if p.side == 1 else row["high"] >= p.stop
            if hit:
                gap = p.entry_i < i and (row["open"] <= p.stop if p.side == 1 else row["open"] >= p.stop)
                close(i, row["open"] if gap else p.stop, "stop_gap" if gap else "stop")
        if position is not None:
            if not force_close_at_19_open and exits[i]:
                close(i, row["close"], "19_utc_bar_close")
            else:
                a = atr[i]
                if a is not None:
                    candidate = row["close"]-position.side*2.5*a
                    if (candidate-position.stop)*position.side > 0:
                        position.stop = candidate
        floating = pnl(position, row["close"], row["close"]) if position else 0.0
        equity = balance+floating
        curve.append({"time_ms": row["time"], "equity_usd": equity})
        peak = max(peak, equity)
        max_dd = min(max_dd, equity/peak-1)
    if position is not None:
        close(hi-1, rows[hi-1]["close"], "period_end")
    by_month = {}
    for point in curve:
        month = datetime.fromtimestamp(point["time_ms"]/1000, timezone.utc).strftime("%Y-%m")
        by_month[month] = point["equity_usd"]
    previous = 1000.0
    positive_months = 0
    for value in by_month.values():
        positive_months += value > previous
        previous = value
    return {"return_pct": (balance/1000-1)*100, "max_dd_pct": max_dd*100,
            "trades": len(ledger), "positive_months": positive_months,
            "months": len(by_month), "top_trade_usd": max((t["pnl_usd"] for t in ledger), default=0),
            "cost_jpy_roundtrip": cost_jpy, "risk_pct": risk_pct, "leverage_cap": leverage_cap,
            "force_close_at_19_open": force_close_at_19_open,
            "ledger": ledger, "equity_curve": curve}


def main() -> None:
    rows = load()
    atr = atr_wilder(rows)
    signals, exits = signal_and_time_exit(rows, atr)
    windows = {"published_trade_span_proxy": ("2023-10-01", "2026-08-12"),
               "author_approx_window": ("2023-10-01", "2026-09-01"),
               "five_year": ("2021-10-01", "2026-10-01"),
               "last_year": ("2025-10-01", "2026-10-01")}
    configs = {"author_risk_leverage": {"risk_pct": 1.0, "leverage_cap": 30.0,
                                         "cost_jpy": .016},
               "our_risk_unlevered_notional": {"risk_pct": .5, "leverage_cap": 1.0,
                                                "cost_jpy": .016},
               "our_risk_2x_cost": {"risk_pct": .5, "leverage_cap": 1.0,
                                     "cost_jpy": .032},
               "author_risk_2x_cost": {"risk_pct": 1.0, "leverage_cap": 30.0,
                                       "cost_jpy": .032}}
    results = {window: {name: replay(rows, atr, signals, exits, start=start, end=end, **params)
                        for name, params in configs.items()}
               for window, (start, end) in windows.items()}
    for window, (start, end) in windows.items():
        results[window]["author_risk_early_19_open"] = replay(
            rows, atr, signals, exits, start=start, end=end, **configs["author_risk_leverage"],
            force_close_at_19_open=True)
    report = {"created_at_utc": datetime.now(timezone.utc).isoformat(),
              "source_repo": "https://github.com/sbrakni/MQL5-trading-bot-claude-experiment",
              "source_commit": UPSTREAM_COMMIT, "source_preset": "ApexBreakout_USDJPY_H1_Session.set",
              "input_csv": str(DATA), "input_sha256": hashlib.sha256(DATA.read_bytes()).hexdigest(),
              "data_kind": "HistData M1 BID aggregated to H1; ASK/rollover absent",
              "windows": windows, "configurations": configs, "results": results}
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n")
    for window, variants in results.items():
        print(window, [(name, round(v["return_pct"], 2), round(v["max_dd_pct"], 2), v["trades"])
                       for name, v in variants.items()])
    print(OUT)


if __name__ == "__main__":
    main()
