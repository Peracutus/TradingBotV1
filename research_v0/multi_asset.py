"""Research-only cross-market signals and one shared allocation/risk gate.

Signals consume *completed* UTC bars. No exchange, MT5, broker or order API is
imported here. A RiskIntent is a model decision, never an executable order.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone

from .core import Candle, Signal, ema
from .indicators import adx_wilder, rsi_wilder

HOUR_MS = 3_600_000
FOUR_HOURS_MS = 4 * HOUR_MS


@dataclass(frozen=True)
class CandidateSignal:
    strategy_id: str
    instrument: str
    bucket: str
    side: int
    observed_bar_time: int
    available_at: int
    stop_distance: float
    exit_rule: str
    reason: str


@dataclass(frozen=True)
class ContractSpec:
    instrument: str
    bucket: str
    contract_units: float  # Base units, or ounces of gold, in one lot.
    minimum_lot: float
    lot_step: float
    minimum_notional_usd: float = 0.0
    margin_fraction: float = 1.0


@dataclass(frozen=True)
class OpenExposure:
    instrument: str
    bucket: str
    notional_usd: float
    stop_risk_usd: float
    margin_usd: float


@dataclass(frozen=True)
class RiskIntent:
    signal: CandidateSignal
    lots: float
    estimated_notional_usd: float
    estimated_stop_risk_usd: float
    estimated_margin_usd: float


@dataclass(frozen=True)
class RiskDecision:
    intent: RiskIntent | None
    reason: str


@dataclass(frozen=True)
class AllocationPolicy:
    crypto_fraction: float = 0.10
    gold_fraction: float = 0.15
    fx_fraction: float = 0.75
    risk_per_trade_fraction: float = 0.005
    total_open_risk_fraction: float = 0.01
    # 1.0 means allocated cash cannot create a larger gross position.
    # A higher value requires a separate, explicit leveraged research scenario.
    max_bucket_gross_multiple: float = 1.0

    def __post_init__(self) -> None:
        shares = (self.crypto_fraction, self.gold_fraction, self.fx_fraction)
        if any(not math.isfinite(x) or x < 0 for x in shares) or not math.isclose(sum(shares), 1.0):
            raise ValueError("Allocation fractions must be nonnegative and sum to 1")
        if not (0 < self.risk_per_trade_fraction <= self.total_open_risk_fraction <= 1):
            raise ValueError("Invalid per-trade or total risk limit")
        if not math.isfinite(self.max_bucket_gross_multiple) or self.max_bucket_gross_multiple <= 0:
            raise ValueError("Invalid gross position multiple")

    def fraction(self, bucket: str) -> float:
        if bucket not in ("crypto", "gold", "fx"):
            raise ValueError(f"Unknown bucket: {bucket}")
        return getattr(self, f"{bucket}_fraction")


class SharedRiskGate:
    """Size a candidate against all existing positions, independent of venue.

    `quote_to_usd` converts one quote currency unit to USD. For XAUUSD and
    EURUSD this is 1; for USDJPY it is approximately 1 / current USDJPY.
    Broker-specific lot, margin, spread and quote data are mandatory inputs.
    """

    def __init__(self, policy: AllocationPolicy | None = None):
        self.policy = policy or AllocationPolicy()

    def approve(self, signal: CandidateSignal, spec: ContractSpec, *,
                decision_time: int, quote_time: int, price: float,
                quote_to_usd: float, equity_usd: float,
                open_positions: tuple[OpenExposure, ...] = ()) -> RiskDecision:
        if signal.bucket != spec.bucket or signal.instrument != spec.instrument:
            return RiskDecision(None, "instrument_or_bucket_mismatch")
        if (signal.side not in (-1, 1) or signal.observed_bar_time > signal.available_at
                or signal.available_at > decision_time):
            return RiskDecision(None, "signal_not_available")
        if quote_time > decision_time or decision_time - quote_time > HOUR_MS:
            return RiskDecision(None, "stale_or_future_quote")
        numbers = (price, quote_to_usd, equity_usd, signal.stop_distance,
                   spec.contract_units, spec.minimum_lot, spec.lot_step,
                   spec.margin_fraction, spec.minimum_notional_usd)
        if (any(not math.isfinite(x) for x in numbers)
                or min(numbers[:-1]) <= 0 or spec.minimum_notional_usd < 0):
            return RiskDecision(None, "invalid_price_contract_or_equity")
        if not (0 < spec.margin_fraction <= 1):
            return RiskDecision(None, "invalid_margin_fraction")
        if any(not all(math.isfinite(x) and x >= 0 for x in
                       (p.notional_usd, p.stop_risk_usd, p.margin_usd))
               or p.bucket not in ("crypto", "gold", "fx") for p in open_positions):
            return RiskDecision(None, "invalid_open_exposure")
        if any(p.instrument == signal.instrument for p in open_positions):
            return RiskDecision(None, "instrument_already_open")

        bucket_limit = equity_usd * self.policy.fraction(signal.bucket)
        used_notional = sum(p.notional_usd for p in open_positions if p.bucket == signal.bucket)
        used_margin = sum(p.margin_usd for p in open_positions if p.bucket == signal.bucket)
        used_risk = sum(p.stop_risk_usd for p in open_positions)
        allowed_risk = min(equity_usd * self.policy.risk_per_trade_fraction,
                           equity_usd * self.policy.total_open_risk_fraction - used_risk)
        allowed_notional = bucket_limit * self.policy.max_bucket_gross_multiple - used_notional
        allowed_margin = bucket_limit - used_margin
        if min(allowed_risk, allowed_notional, allowed_margin) <= 0:
            return RiskDecision(None, "shared_risk_or_bucket_exhausted")

        notional_per_lot = price * spec.contract_units * quote_to_usd
        risk_per_lot = signal.stop_distance * spec.contract_units * quote_to_usd
        margin_per_lot = notional_per_lot * spec.margin_fraction
        raw_lots = min(allowed_risk / risk_per_lot,
                       allowed_notional / notional_per_lot,
                       allowed_margin / margin_per_lot)
        steps = math.floor((raw_lots + 1e-12) / spec.lot_step)
        lots = steps * spec.lot_step
        if lots + 1e-12 < spec.minimum_lot or lots * notional_per_lot < spec.minimum_notional_usd:
            return RiskDecision(None, "minimum_lot_exceeds_allocation_or_risk")
        intent = RiskIntent(signal, lots, lots * notional_per_lot,
                            lots * risk_per_lot, lots * margin_per_lot)
        return RiskDecision(intent, "approved_research_intent")


def crypto_baseline_candidate(signal: Signal, previous_closed_atr: float,
                              stop_atr_multiple: float = 2.0) -> CandidateSignal:
    """Bridge the frozen crypto EMA signal into the common risk vocabulary."""
    if signal.direction != 1 or previous_closed_atr <= 0 or stop_atr_multiple <= 0:
        raise ValueError("The preserved spot baseline requires long and a positive stop distance")
    return CandidateSignal(signal.strategy_id, signal.instrument, "crypto", 1,
                           signal.observed_bar_time, signal.available_at,
                           stop_atr_multiple * previous_closed_atr,
                           "existing_crypto_baseline_exit_or_stop", signal.reason)


def _require_complete(bars: list[Candle], interval_ms: int, decision_time: int, count: int) -> bool:
    return (len(bars) >= count and bars[-1].time + interval_ms <= decision_time
            and decision_time < bars[-1].time + 2 * interval_ms
            and all(b.time + interval_ms <= decision_time for b in bars[-count:]))


def _hour_utc(ms: int) -> int:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).hour


def _date_utc(ms: int):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).date()


def _atr_wilder(bars: list[Candle], period: int = 14) -> float:
    if len(bars) < period + 1:
        return 0.0
    tr = []
    for i, b in enumerate(bars):
        previous = bars[i - 1].close if i else b.close
        tr.append(max(b.high - b.low, abs(b.high - previous), abs(b.low - previous)))
    value = sum(tr[1:period + 1]) / period
    for x in tr[period + 1:]:
        value = (value * (period - 1) + x) / period
    return value


def claude_h4_h1_candidate(instrument: str, h4: list[Candle], h1: list[Candle],
                           decision_time: int) -> CandidateSignal | None:
    """Point-in-time adaptation of the published H4 trend / H1 entry idea.

    Research candidate, not a claim of exact Claude Trader reproduction.
    Gold uses H1 trend confirmation; EUR/GBP use H1 stochastic pullbacks.
    """
    if instrument not in ("XAUUSD", "EURUSD", "GBPUSD"):
        raise ValueError("Claude H4/H1 candidate supports gold, EURUSD and GBPUSD")
    if not (_require_complete(h4, FOUR_HOURS_MS, decision_time, 201)
            and _require_complete(h1, HOUR_MS, decision_time, 50)):
        return None
    close4 = [b.close for b in h4]
    ema20, ema50, ema200 = (ema(close4, n)[-1] for n in (20, 50, 200))
    adx = adx_wilder(h4)
    rsi4 = rsi_wilder(h4)[-1]
    adx_min = {"XAUUSD": 25, "EURUSD": 22, "GBPUSD": 28}[instrument]
    if not (adx[-1] > adx_min and adx[-1] > adx[-2]):
        return None
    bull = close4[-1] > ema20 > ema50 > ema200
    bear = close4[-1] < ema20 < ema50 < ema200
    threshold = 55 if instrument == "GBPUSD" else 58
    side = 1 if bull and rsi4 > threshold else (-1 if bear and rsi4 < 100 - threshold else 0)
    if not side:
        return None
    close1 = [b.close for b in h1]
    if instrument == "XAUUSD":
        h1_ema = ema(close1, 20)[-1]
        h1_rsi = rsi_wilder(h1)[-1]
        confirmed = (close1[-1] > h1_ema and h1_rsi > 55) if side == 1 else (close1[-1] < h1_ema and h1_rsi < 45)
        stop_multiple = 1.2
    else:
        h1_ema = ema(close1, 50)[-1]
        tail = h1[-14:]
        low, high = min(b.low for b in tail), max(b.high for b in tail)
        if high <= low:
            return None
        stoch = 100 * (close1[-1] - low) / (high - low)
        confirmed = (stoch < 25 and close1[-1] > h1_ema) if side == 1 else (stoch > 75 and close1[-1] < h1_ema)
        stop_multiple = 1.2 if instrument == "EURUSD" else 1.5
    if not confirmed:
        return None
    atr = _atr_wilder(h4)
    floor = {"XAUUSD": 20.0, "EURUSD": 0.002, "GBPUSD": 0.003}[instrument]
    return CandidateSignal("claude_h4_h1_research", instrument,
                           "gold" if instrument == "XAUUSD" else "fx", side,
                           h1[-1].time, h1[-1].time + HOUR_MS,
                           max(stop_multiple * atr, floor), "2.5R_or_60_M15_bar_timeout",
                           "completed_H4_trend_and_completed_H1_entry")


def apex_usdjpy_session_candidate(h1: list[Candle], decision_time: int) -> CandidateSignal | None:
    """Apex v1: complete 00–07 UTC range, 07–12 close beyond 0.25 ATR."""
    if not _require_complete(h1, HOUR_MS, decision_time, 15):
        return None
    last = h1[-1]
    day = _date_utc(last.time)
    hour = _hour_utc(last.time)
    if not 7 <= hour < 12:
        return None
    today = { _hour_utc(b.time): b for b in h1 if _date_utc(b.time) == day }
    if any(h not in today for h in range(7)):
        return None  # A missing Asian-session hour invalidates this day's range.
    high = max(today[h].high for h in range(7))
    low = min(today[h].low for h in range(7))
    atr = _atr_wilder(h1)
    if atr <= 0:
        return None
    side = 1 if last.close > high + .25 * atr else (-1 if last.close < low - .25 * atr else 0)
    if side == 0:
        return None
    # The published rule permits at most one signal per direction per day.
    for h in range(7, hour):
        b = today.get(h)
        if b is None:
            return None
        prior_atr = _atr_wilder([x for x in h1 if x.time <= b.time])
        if side == 1 and b.close > high + .25 * prior_atr:
            return None
        if side == -1 and b.close < low - .25 * prior_atr:
            return None
    return CandidateSignal("apex_usdjpy_h1_session_v1", "USDJPY", "fx", side,
                           last.time, last.time + HOUR_MS, 2 * atr,
                           "trail_2.5_ATR_then_19_UTC_bar_close",
                           "first_completed_H1_close_beyond_Asian_range")


def apex_gold_donchian_candidate(h1: list[Candle], decision_time: int) -> CandidateSignal | None:
    """Gold Donchian 24/12 + EMA200 idea; independent unverified candidate."""
    if not _require_complete(h1, HOUR_MS, decision_time, 201):
        return None
    last = h1[-1]
    upper = max(b.high for b in h1[-25:-1])
    lower = min(b.low for b in h1[-25:-1])
    trend = ema([b.close for b in h1], 200)[-1]
    side = 1 if last.close > upper and last.close > trend else (-1 if last.close < lower and last.close < trend else 0)
    if not side:
        return None
    atr = _atr_wilder(h1)
    if atr <= 0:
        return None
    return CandidateSignal("apex_xauusd_h1_donchian24_12", "XAUUSD", "gold", side,
                           last.time, last.time + HOUR_MS, 2.5 * atr,
                           "opposite_12_bar_channel_or_initial_stop",
                           "completed_H1_Donchian24_breakout_EMA200")
