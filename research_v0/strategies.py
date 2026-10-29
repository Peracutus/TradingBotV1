"""Prespecified BTC/ETH research strategies; one central configuration each."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .core import Candle, Features, Signal, features
from .indicators import adx_wilder, bollinger, fixed_train_quantile, rsi_wilder

PRE_PERIOD_END_MS = int(datetime(2025, 10, 5, tzinfo=timezone.utc).timestamp() * 1000)


def emit(strategy_id: str, instrument: str, candles: list[Candle], i: int, reason: str) -> Signal:
    interval = candles[1].time - candles[0].time
    return Signal(strategy_id, instrument, candles[i].time, candles[i].time + interval, 1, reason)


@dataclass(frozen=True)
class H01EmaTrend:
    strategy_id: str = "H01_ema20_50_trend200"
    warmup_bars: int = 201
    stop_atr_multiple: float = 2.0

    def prepare(self, candles: list[Candle]) -> Features:
        return features(candles, 20, 50, 200)

    def evaluate(self, instrument: str, candles: list[Candle], v: Features, i: int) -> Signal | None:
        if i < 200:
            return None
        if v.fast[i - 1] <= v.slow[i - 1] and v.fast[i] > v.slow[i] and candles[i].close > v.trend[i]:
            return emit(self.strategy_id, instrument, candles, i, "ema_cross")
        return None

    def exit_signal(self, instrument: str, candles: list[Candle], v: Features, i: int) -> bool:
        return v.fast[i - 1] >= v.slow[i - 1] and v.fast[i] < v.slow[i]


@dataclass(frozen=True)
class H02ConfirmedMa:
    strategy_id: str = "H02_two_closes_above_ema50"
    warmup_bars: int = 201
    stop_atr_multiple: float = 2.0

    def prepare(self, candles: list[Candle]) -> Features:
        return features(candles, 20, 50, 200)

    def evaluate(self, instrument: str, candles: list[Candle], v: Features, i: int) -> Signal | None:
        if i < 200:
            return None
        if (candles[i - 2].close <= v.slow[i - 2] and candles[i - 1].close > v.slow[i - 1]
                and candles[i].close > v.slow[i] and candles[i].close > v.trend[i]):
            return emit(self.strategy_id, instrument, candles, i, "two_confirmed_closes")
        return None

    def exit_signal(self, instrument: str, candles: list[Candle], v: Features, i: int) -> bool:
        return candles[i].close < v.slow[i]


@dataclass(frozen=True)
class H03NormalizedSlope:
    strategy_id: str = "H03_ema50_cross_slope025"
    warmup_bars: int = 231
    stop_atr_multiple: float = 2.0

    def prepare(self, candles: list[Candle]) -> Features:
        return features(candles, 20, 50, 200)

    def evaluate(self, instrument: str, candles: list[Candle], v: Features, i: int) -> Signal | None:
        if i < 230 or v.atr[i] <= 0:
            return None
        slope = (v.trend[i] - v.trend[i - 30]) / v.atr[i]
        if (candles[i - 1].close <= v.slow[i - 1] and candles[i].close > v.slow[i]
                and candles[i].close > v.trend[i] and slope > 0.25):
            return emit(self.strategy_id, instrument, candles, i, "price_cross_normalized_slope")
        return None

    def exit_signal(self, instrument: str, candles: list[Candle], v: Features, i: int) -> bool:
        return candles[i].close < v.slow[i]


@dataclass(frozen=True)
class H04Donchian:
    strategy_id: str = "H04_donchian20_10"
    warmup_bars: int = 201
    stop_atr_multiple: float = 2.0

    def prepare(self, candles: list[Candle]) -> Features:
        return features(candles, 20, 50, 200)

    def evaluate(self, instrument: str, candles: list[Candle], v: Features, i: int) -> Signal | None:
        if i < 20:
            return None
        prior_high = max(c.high for c in candles[i - 20:i])
        if candles[i].close > prior_high:
            return emit(self.strategy_id, instrument, candles, i, "donchian20_break")
        return None

    def exit_signal(self, instrument: str, candles: list[Candle], v: Features, i: int) -> bool:
        return candles[i].close < min(c.low for c in candles[i - 10:i])


@dataclass(frozen=True)
class H05DailyMomentum:
    strategy_id: str = "H05_daily_roc20_ema100"
    warmup_bars: int = 101
    stop_atr_multiple: float = 2.0

    def prepare(self, candles: list[Candle]) -> Features:
        return features(candles, 20, 50, 100)

    def evaluate(self, instrument: str, candles: list[Candle], v: Features, i: int) -> Signal | None:
        if i < 100:
            return None
        roc = candles[i].close / candles[i - 20].close - 1
        if roc > 0 and candles[i].close > v.trend[i]:
            return emit(self.strategy_id, instrument, candles, i, "daily_roc20_positive")
        return None

    def exit_signal(self, instrument: str, candles: list[Candle], v: Features, i: int) -> bool:
        return candles[i].close / candles[i - 20].close - 1 < 0


@dataclass(frozen=True)
class H06DailyMomentumAcceleration:
    strategy_id: str = "H06_daily_roc30_vs_roc90"
    warmup_bars: int = 91
    stop_atr_multiple: float = 2.0

    def prepare(self, candles: list[Candle]) -> Features:
        return features(candles, 20, 50, 100)

    def _condition(self, candles: list[Candle], i: int) -> bool:
        if i < 90:
            return False
        roc30 = candles[i].close / candles[i - 30].close - 1
        roc90 = candles[i].close / candles[i - 90].close - 1
        return roc30 > 0 and roc90 > 0 and roc30 > roc90 / 3

    def evaluate(self, instrument: str, candles: list[Candle], v: Features, i: int) -> Signal | None:
        return emit(self.strategy_id, instrument, candles, i, "momentum_acceleration") if self._condition(candles, i) else None

    def exit_signal(self, instrument: str, candles: list[Candle], v: Features, i: int) -> bool:
        return not self._condition(candles, i) and not self._condition(candles, i - 1)


@dataclass(frozen=True)
class H08BollingerRange:
    strategy_id: str = "H08_bb20_reentry_adx_train30pct"
    warmup_bars: int = 201
    stop_atr_multiple: float = 2.0
    train_end_ms: int = PRE_PERIOD_END_MS

    def prepare(self, candles: list[Candle]) -> Features:
        base = features(candles, 20, 50, 200)
        mid, low, high, width = bollinger(candles)
        adx = adx_wilder(candles)
        threshold = fixed_train_quantile(candles, adx, self.train_end_ms, 0.30)
        return Features(base.fast, base.slow, base.trend, base.atr,
                        {"bb_mid": mid, "bb_low": low, "adx": adx,
                         "train_adx_threshold": [threshold] * len(candles)})

    def evaluate(self, instrument: str, candles: list[Candle], v: Features, i: int) -> Signal | None:
        ex = v.extras
        if (candles[i - 1].close < ex["bb_low"][i - 1] and candles[i].close >= ex["bb_low"][i]
                and ex["adx"][i] < ex["train_adx_threshold"][i]):
            return emit(self.strategy_id, instrument, candles, i, "bb_return_inside_low_adx")
        return None

    def exit_signal(self, instrument: str, candles: list[Candle], v: Features, i: int) -> bool:
        return candles[i].close >= v.extras["bb_mid"][i]


@dataclass(frozen=True)
class H09RsiRange:
    strategy_id: str = "H09_rsi14_below25_adx_train30pct"
    warmup_bars: int = 201
    stop_atr_multiple: float = 2.0
    train_end_ms: int = PRE_PERIOD_END_MS

    def prepare(self, candles: list[Candle]) -> Features:
        base = features(candles, 20, 50, 200)
        adx = adx_wilder(candles)
        threshold = fixed_train_quantile(candles, adx, self.train_end_ms, 0.30)
        return Features(base.fast, base.slow, base.trend, base.atr,
                        {"rsi": rsi_wilder(candles), "adx": adx,
                         "train_adx_threshold": [threshold] * len(candles)})

    def evaluate(self, instrument: str, candles: list[Candle], v: Features, i: int) -> Signal | None:
        ex = v.extras
        if (ex["rsi"][i] < 25 and candles[i].close > candles[i - 1].close
                and ex["adx"][i] < ex["train_adx_threshold"][i]):
            return emit(self.strategy_id, instrument, candles, i, "rsi_oversold_rebound_low_adx")
        return None

    def exit_signal(self, instrument: str, candles: list[Candle], v: Features, i: int) -> bool:
        return v.extras["rsi"][i] > 50


@dataclass(frozen=True)
class H10VolatilityCompression:
    strategy_id: str = "H10_bbwidth_train20pct_donchian20"
    warmup_bars: int = 201
    stop_atr_multiple: float = 2.0
    train_end_ms: int = PRE_PERIOD_END_MS

    def prepare(self, candles: list[Candle]) -> Features:
        base = features(candles, 20, 50, 200)
        mid, low, high, width = bollinger(candles)
        threshold = fixed_train_quantile(candles, width, self.train_end_ms, 0.20)
        return Features(base.fast, base.slow, base.trend, base.atr,
                        {"bb_width": width, "train_width_threshold": [threshold] * len(candles)})

    def evaluate(self, instrument: str, candles: list[Candle], v: Features, i: int) -> Signal | None:
        ex = v.extras
        if (ex["bb_width"][i] < ex["train_width_threshold"][i]
                and candles[i].close > max(c.high for c in candles[i - 20:i])):
            return emit(self.strategy_id, instrument, candles, i, "compressed_donchian_break")
        return None

    def exit_signal(self, instrument: str, candles: list[Candle], v: Features, i: int) -> bool:
        return candles[i].close < v.fast[i]
