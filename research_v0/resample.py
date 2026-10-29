"""Point-in-time aggregation of complete UTC days from contiguous 4h bars."""
from __future__ import annotations

from .core import Candle

DAY_MS = 86_400_000
FOUR_HOURS_MS = 14_400_000


def complete_utc_days(candles: list[Candle]) -> list[Candle]:
    if not candles:
        return []
    groups: dict[int, list[Candle]] = {}
    for candle in candles:
        day = candle.time // DAY_MS * DAY_MS
        groups.setdefault(day, []).append(candle)
    output = []
    for day, bars in groups.items():
        expected = [day + i * FOUR_HOURS_MS for i in range(6)]
        actual = [bar.time for bar in bars]
        if actual != expected:
            if day in (next(iter(groups)), next(reversed(groups))):
                continue  # Drop incomplete boundary days, never synthesize candles.
            raise ValueError(f"incomplete interior UTC day {day}")
        output.append(Candle(day, bars[0].open, max(b.high for b in bars),
                             min(b.low for b in bars), bars[-1].close,
                             sum(b.volume for b in bars)))
    return output
