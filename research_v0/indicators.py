"""Closed-bar technical indicators with no future-window reads."""
from __future__ import annotations

import math

from .core import Candle


def bollinger(candles: list[Candle], n: int = 20, sigma: float = 2.0) -> tuple[list[float], list[float], list[float], list[float]]:
    close = [c.close for c in candles]
    middle, lower, upper, width = [], [], [], []
    for i in range(len(close)):
        sample = close[max(0, i - n + 1):i + 1]
        mean = sum(sample) / len(sample)
        std = math.sqrt(sum((x - mean) ** 2 for x in sample) / len(sample))
        middle.append(mean)
        lower.append(mean - sigma * std)
        upper.append(mean + sigma * std)
        width.append(2 * sigma * std / mean if mean > 0 else 0.0)
    return middle, lower, upper, width


def rsi_wilder(candles: list[Candle], n: int = 14) -> list[float]:
    close = [c.close for c in candles]
    out = [50.0] * len(close)
    if len(close) <= n:
        return out
    gains = [max(close[i] - close[i - 1], 0.0) for i in range(1, len(close))]
    losses = [max(close[i - 1] - close[i], 0.0) for i in range(1, len(close))]
    gain = sum(gains[:n]) / n
    loss = sum(losses[:n]) / n
    for i in range(n, len(close)):
        if i > n:
            gain = (gain * (n - 1) + gains[i - 1]) / n
            loss = (loss * (n - 1) + losses[i - 1]) / n
        out[i] = 100.0 if loss == 0 and gain > 0 else (50.0 if loss == gain == 0 else 100 - 100 / (1 + gain / loss))
    return out


def adx_wilder(candles: list[Candle], n: int = 14) -> list[float]:
    length = len(candles)
    out = [0.0] * length
    if length < 2 * n:
        return out
    tr = [0.0] * length
    plus_dm = [0.0] * length
    minus_dm = [0.0] * length
    for i in range(1, length):
        curr, prev = candles[i], candles[i - 1]
        up, down = curr.high - prev.high, prev.low - curr.low
        plus_dm[i] = up if up > down and up > 0 else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
        tr[i] = max(curr.high - curr.low, abs(curr.high - prev.close), abs(curr.low - prev.close))
    sm_tr = sum(tr[1:n + 1])
    sm_plus = sum(plus_dm[1:n + 1])
    sm_minus = sum(minus_dm[1:n + 1])
    dx = [0.0] * length
    for i in range(n, length):
        if i > n:
            sm_tr = sm_tr - sm_tr / n + tr[i]
            sm_plus = sm_plus - sm_plus / n + plus_dm[i]
            sm_minus = sm_minus - sm_minus / n + minus_dm[i]
        plus_di = 100 * sm_plus / sm_tr if sm_tr > 0 else 0.0
        minus_di = 100 * sm_minus / sm_tr if sm_tr > 0 else 0.0
        dx[i] = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if plus_di + minus_di > 0 else 0.0
    first_adx_index = 2 * n - 1
    out[first_adx_index] = sum(dx[n:first_adx_index + 1]) / n
    for i in range(first_adx_index + 1, length):
        out[i] = (out[i - 1] * (n - 1) + dx[i]) / n
    return out


def fixed_train_quantile(candles: list[Candle], values: list[float], end_exclusive_ms: int,
                         q: float, minimum_index: int = 50) -> float:
    sample = sorted(values[i] for i, c in enumerate(candles)
                    if minimum_index <= i and c.time < end_exclusive_ms and math.isfinite(values[i]))
    if len(sample) < 30:
        raise ValueError("insufficient pre-period bars to freeze regime quantile")
    position = q * (len(sample) - 1)
    lo = math.floor(position)
    hi = math.ceil(position)
    return sample[lo] + (sample[hi] - sample[lo]) * (position - lo)
