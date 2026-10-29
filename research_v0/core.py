"""Deterministic, point-in-time BTC/ETH spot research engine."""
from __future__ import annotations

import csv
import hashlib
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol


def utc_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()


@dataclass(frozen=True)
class Candle:
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class DatasetManifest:
    provider: str | None
    venue: str | None
    instrument: str
    data_type: str
    timezone: str
    downloaded_at: str | None
    coverage_start: str
    coverage_end: str
    adjustment_policy: str
    raw_file_hash: str
    provenance_notes: str


def load_candles(path: Path, instrument: str, *, interval_ms: int = 14_400_000) -> tuple[list[Candle], DatasetManifest]:
    raw = path.read_bytes()
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows:
        raise ValueError(f"{path}: empty dataset")
    candles = [Candle(int(r["time"]), *(float(r[k]) for k in ("open", "high", "low", "close", "volume"))) for r in rows]
    for i, c in enumerate(candles):
        if not all(math.isfinite(v) for v in (c.open, c.high, c.low, c.close, c.volume)):
            raise ValueError(f"{path}: nonfinite candle {i}")
        if min(c.open, c.high, c.low, c.close) <= 0 or c.volume < 0 or c.low > min(c.open, c.close) or c.high < max(c.open, c.close):
            raise ValueError(f"{path}: impossible OHLCV {i}")
        if i and c.time - candles[i - 1].time != interval_ms:
            raise ValueError(f"{path}: gap/duplicate/nonmonotonic timestamp at {i}")
    return candles, DatasetManifest(
        provider=None,
        venue="Bybit spot (legacy label; not independently verified)",
        instrument=instrument,
        data_type="OHLCV 4h", timezone="UTC", downloaded_at=None,
        coverage_start=utc_ms(candles[0].time), coverage_end=utc_ms(candles[-1].time),
        adjustment_policy="unknown in legacy export", raw_file_hash=hashlib.sha256(raw).hexdigest(),
        provenance_notes="Source venue and download time unavailable in original CSV; no bid/ask or order book.",
    )


def ema(values: list[float], n: int) -> list[float]:
    alpha = 2 / (n + 1)
    z = values[0]
    out = []
    for value in values:
        z = alpha * value + (1 - alpha) * z
        out.append(z)
    return out


@dataclass(frozen=True)
class Features:
    fast: list[float]
    slow: list[float]
    trend: list[float]
    atr: list[float]
    extras: dict[str, list[float]] = field(default_factory=dict)


def features(candles: list[Candle], fast: int, slow: int, trend: int) -> Features:
    closes = [c.close for c in candles]
    tr = []
    for i, c in enumerate(candles):
        prior = closes[i - 1] if i else c.close
        tr.append(max(c.high - c.low, abs(c.high - prior), abs(c.low - prior)))
    return Features(ema(closes, fast), ema(closes, slow), ema(closes, trend), ema(tr, 14))


@dataclass(frozen=True)
class Signal:
    strategy_id: str
    instrument: str
    observed_bar_time: int
    available_at: int
    direction: int
    reason: str


class Strategy(Protocol):
    warmup_bars: int
    stop_atr_multiple: float
    def prepare(self, candles: list[Candle]) -> Features: ...
    def evaluate(self, instrument: str, candles: list[Candle], values: Features, closed_index: int) -> Signal | None: ...
    def exit_signal(self, instrument: str, candles: list[Candle], values: Features, closed_index: int) -> bool: ...


@dataclass(frozen=True)
class EmaCrossStrategy:
    strategy_id: str = "ema30_100_trend200_stop2"
    warmup_bars: int = 201
    stop_atr_multiple: float = 2.0

    def prepare(self, candles: list[Candle]) -> Features:
        return features(candles, 30, 100, 200)

    def evaluate(self, instrument: str, candles: list[Candle], values: Features, closed_index: int) -> Signal | None:
        i = closed_index
        if i < 200:
            return None
        if values.fast[i - 1] <= values.slow[i - 1] and values.fast[i] > values.slow[i] and candles[i].close > values.trend[i]:
            return Signal(self.strategy_id, instrument, candles[i].time, candles[i].time + 14_400_000, 1, "ema_cross")
        return None

    def exit_signal(self, instrument: str, candles: list[Candle], values: Features, closed_index: int) -> bool:
        i = closed_index
        return values.fast[i - 1] >= values.slow[i - 1] and values.fast[i] < values.slow[i]


@dataclass(frozen=True)
class InstrumentSpec:
    instrument: str
    minimum_notional: float = 5.0
    minimum_lot: float = 0.0
    lot_step: float = 0.0
    maximum_participation: float | None = None
    depth_notional: float | None = None
    maximum_deployable_capital: float | None = None
    slippage_curve_bps_per_1pct_participation: float | None = None


@dataclass(frozen=True)
class CostModel:
    fee_each_side: float = 0.001
    slippage_each_side: float = 0.0005
    funding_per_bar: float = 0.0  # spot only in V0


@dataclass(frozen=True)
class OrderIntent:
    instrument: str
    quantity: float
    reference_open: float
    stop_distance: float
    observed_bar_time: int
    execution_bar_time: int
    estimated_participation: float | None
    estimated_slippage_each_side: float


@dataclass
class Position:
    quantity: float
    entry: float
    stop: float
    cost: float
    entry_time: int


class PortfolioRisk:
    def __init__(self, specs: dict[str, InstrumentSpec], risk_fraction: float = 0.005, total_allocation: float = 0.70):
        self.specs = specs
        self.risk_fraction = risk_fraction
        self.total_allocation = total_allocation

    def approve(self, signal: Signal, current: Candle, atr_previous: float, prior_volume: float, cash: float, equity: float,
                positions: dict[str, Position | None], costs: CostModel, stop_atr_multiple: float = 2.0) -> OrderIntent | None:
        if signal.available_at > current.time or signal.direction != 1:
            return None
        spec = self.specs[signal.instrument]
        price = current.open * (1 + costs.slippage_each_side)
        distance = stop_atr_multiple * atr_previous
        if distance <= 0:
            return None
        used = sum(p.quantity * max(0, p.entry - p.stop) for p in positions.values() if p)
        qty = min(max(0, equity * self.risk_fraction - used) / distance,
                  equity * self.total_allocation / len(self.specs) / (price * (1 + costs.fee_each_side)),
                  cash / (price * (1 + costs.fee_each_side)))
        if spec.maximum_deployable_capital is not None:
            qty = min(qty, spec.maximum_deployable_capital / price)
        participation = qty / prior_volume if prior_volume > 0 else None
        if spec.maximum_participation is not None:
            if participation is None:
                return None
            qty = min(qty, spec.maximum_participation * prior_volume)
        if spec.depth_notional is not None:
            qty = min(qty, spec.depth_notional / price)
        if spec.lot_step:
            qty = math.floor(qty / spec.lot_step) * spec.lot_step
        participation = qty / prior_volume if prior_volume > 0 else None
        if qty < spec.minimum_lot or qty * price < spec.minimum_notional:
            return None
        # Only previous closed-bar volume is observable at order decision time.
        # The curve is recorded, but not applied in the frozen baseline fixture.
        impact = 0.0
        if spec.slippage_curve_bps_per_1pct_participation is not None:
            if participation is None:
                return None
            impact = spec.slippage_curve_bps_per_1pct_participation * (participation / 0.01) / 10_000
        return OrderIntent(signal.instrument, qty, current.open, distance, signal.observed_bar_time,
                           current.time, participation, costs.slippage_each_side + impact)


class ResearchExecution:
    """Simulated fills only; intentionally has no broker credentials or API calls."""
    def __init__(self, costs: CostModel):
        self.costs = costs

    def buy(self, intent: OrderIntent, candle: Candle) -> Position:
        price = candle.open * (1 + intent.estimated_slippage_each_side)
        return Position(intent.quantity, price, price - intent.stop_distance,
                        intent.quantity * price * (1 + self.costs.fee_each_side), candle.time)

    def sell(self, position: Position, reference_price: float, *, extra_slippage: float = 0.0) -> float:
        return position.quantity * reference_price * (1 - self.costs.slippage_each_side - extra_slippage) * (1 - self.costs.fee_each_side)


def impact_from_previous_volume(spec: InstrumentSpec, quantity: float, prior_volume: float) -> float:
    if spec.slippage_curve_bps_per_1pct_participation is None:
        return 0.0
    if prior_volume <= 0:
        raise ValueError("impact curve requires previous closed-bar volume")
    return spec.slippage_curve_bps_per_1pct_participation * (quantity / prior_volume / 0.01) / 10_000


class ExecutionAdapter(Protocol):
    def buy(self, intent: OrderIntent, candle: Candle) -> Position: ...
    def sell(self, position: Position, reference_price: float, *, extra_slippage: float = 0.0) -> float: ...


@dataclass(frozen=True)
class Trade:
    instrument: str
    entry_time: int
    exit_time: int
    entry: float
    exit_reference: float
    quantity: float
    pnl: float
    exit_reason: str


@dataclass
class TradeLedger:
    trades: list[Trade]

    def record(self, trade: Trade) -> None:
        self.trades.append(trade)


@dataclass
class BacktestResult:
    return_pct: float
    max_drawdown_pct: float
    trades: list[Trade]
    equity: list[float]
    end_equity: float
    signals: list[Signal]


def backtest(data: dict[str, list[Candle]], lo: int, hi: int, *, strategy: Strategy | None = None,
             costs: CostModel | None = None, specs: dict[str, InstrumentSpec] | None = None,
             start_equity: float = 1000.0) -> BacktestResult:
    strategy = strategy or EmaCrossStrategy()
    costs = costs or CostModel()
    specs = specs or {name: InstrumentSpec(name) for name in data}
    feat = {name: strategy.prepare(rows) for name, rows in data.items()}
    timeline = data["BTC"]
    if any([c.time for c in rows] != [c.time for c in timeline] for rows in data.values()):
        raise ValueError("instruments require identical timestamps")
    risk = PortfolioRisk(specs)
    execution = ResearchExecution(costs)
    cash = start_equity
    positions: dict[str, Position | None] = {name: None for name in data}
    equity_curve: list[float] = []
    ledger = TradeLedger([])
    signals: list[Signal] = []
    last_exit = {name: -99999 for name in data}
    for i in range(max(strategy.warmup_bars, lo + 1), min(hi, len(timeline))):
        prev = i - 1
        for name, rows in data.items():
            p = positions[name]
            if p is None:
                continue
            candle = rows[i]
            cross = strategy.exit_signal(name, rows, feat[name], prev)
            stop_hit = candle.low <= p.stop
            if cross or stop_hit:
                reference = min(candle.open, p.stop) if stop_hit else candle.open
                proceeds = execution.sell(p, reference, extra_slippage=impact_from_previous_volume(specs[name], p.quantity,
                                                                                                   rows[prev].volume))
                cash += proceeds
                ledger.record(Trade(name, p.entry_time, candle.time, p.entry, reference, p.quantity,
                                    proceeds - p.cost, "stop" if stop_hit else "ema_cross"))
                positions[name] = None
                last_exit[name] = i
        for name, rows in data.items():
            if positions[name] is not None or i - last_exit[name] <= 0:
                continue
            signal = strategy.evaluate(name, rows, feat[name], prev)
            if signal is None:
                continue
            signals.append(signal)
            equity = cash + sum(p.quantity * data[n][i].close for n, p in positions.items() if p)
            intent = risk.approve(signal, rows[i], feat[name].atr[prev], rows[prev].volume,
                                  cash, equity, positions, costs, strategy.stop_atr_multiple)
            if intent is None:
                continue
            p = execution.buy(intent, rows[i])
            if p.cost > cash + 1e-10:
                continue
            cash -= p.cost
            positions[name] = p
            if rows[i].low <= p.stop:
                proceeds = execution.sell(p, p.stop, extra_slippage=impact_from_previous_volume(specs[name], p.quantity,
                                                                                                 rows[prev].volume))
                cash += proceeds
                ledger.record(Trade(name, p.entry_time, rows[i].time, p.entry, p.stop, p.quantity,
                                    proceeds - p.cost, "same_bar_stop"))
                positions[name] = None
                last_exit[name] = i
        equity_curve.append(cash + sum(p.quantity * data[name][i].close for name, p in positions.items() if p))
    for name, p in positions.items():
        if p:
            reference = data[name][hi - 1].close
            # Preserve legacy final liquidation convention exactly for fixture comparison.
            extra = impact_from_previous_volume(specs[name], p.quantity, data[name][hi - 2].volume)
            proceeds = p.quantity * reference * (1 - costs.fee_each_side - costs.slippage_each_side - extra)
            cash += proceeds
            ledger.record(Trade(name, p.entry_time, data[name][hi - 1].time, p.entry, reference,
                                p.quantity, proceeds - p.cost, "period_end"))
    peak = start_equity
    drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        drawdown = min(drawdown, value / peak - 1)
    return BacktestResult((cash / start_equity - 1) * 100, drawdown * 100, ledger.trades, equity_curve, cash, signals)
