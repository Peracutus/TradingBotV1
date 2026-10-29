"""Signal diagnostics and multiple-testing diagnostics for research runs."""
from __future__ import annotations

import itertools
import math
import statistics
from dataclasses import asdict
from statistics import NormalDist

from .core import BacktestResult, Candle, CostModel, EmaCrossStrategy, Strategy


def signal_quality(data: dict[str, list[Candle]], lo: int, hi: int, *, horizon_bars: int = 6,
                   costs: CostModel | None = None, strategy: Strategy | None = None) -> dict:
    """Fixed-horizon probe, independent of position sizing and stops."""
    costs = costs or CostModel()
    strategy = strategy or EmaCrossStrategy()
    gross, net = [], []
    by_instrument = {}
    for name, candles in data.items():
        f = strategy.prepare(candles)
        sample = []
        for i in range(max(lo, strategy.warmup_bars - 1), min(hi - horizon_bars - 1, len(candles) - horizon_bars - 1)):
            if strategy.evaluate(name, candles, f, i) is None:
                continue
            entry = candles[i + 1].open
            exit_price = candles[i + 1 + horizon_bars].open
            raw = exit_price / entry - 1
            after = exit_price * (1 - costs.slippage_each_side) * (1 - costs.fee_each_side) / (
                entry * (1 + costs.slippage_each_side) * (1 + costs.fee_each_side)) - 1
            gross.append(raw)
            net.append(after)
            sample.append(raw)
        by_instrument[name] = {"signal_count": len(sample), "mean_forward_return_pct": 100 * statistics.mean(sample) if sample else None}
    return {
        "horizon_bars": horizon_bars,
        "signal_count": len(gross),
        "direction_hit_rate": sum(r > 0 for r in gross) / len(gross) if gross else None,
        "mean_gross_forward_return_pct": 100 * statistics.mean(gross) if gross else None,
        "mean_after_roundtrip_cost_pct": 100 * statistics.mean(net) if net else None,
        "by_instrument": by_instrument,
        "interpretation": "Offline fixed-horizon signal probe; no sizing, stop, or trade overlap constraints.",
    }


def performance(result: BacktestResult, *, start_equity: float = 1000, bars_per_day: int = 6) -> dict:
    trades = [t.pnl for t in result.trades]
    gains = sum(max(0, p) for p in trades)
    losses = -sum(min(0, p) for p in trades)
    daily = []
    curve = [start_equity] + result.equity.copy()
    if len(curve) > 1:
        curve[-1] = result.end_equity  # Include final liquidation costs in risk ratios.
    for i in range(bars_per_day, len(curve), bars_per_day):
        daily.append(curve[i] / curve[i - bars_per_day] - 1)
    if len(daily) > 1 and statistics.stdev(daily) > 0:
        sharpe = math.sqrt(365) * statistics.mean(daily) / statistics.stdev(daily)
    else:
        sharpe = None
    downside = [min(0.0, r) for r in daily]
    downside_scale = math.sqrt(sum(r * r for r in downside) / len(downside)) if downside else 0
    sortino = math.sqrt(365) * statistics.mean(daily) / downside_scale if downside_scale > 0 else None
    # A full 365-day span is required; shorter windows have no CAGR/Calmar.
    days = len(result.equity) / bars_per_day
    cagr = ((result.end_equity / start_equity) ** (365 / days) - 1) if days >= 365 else None
    calmar = cagr / abs(result.max_drawdown_pct / 100) if cagr is not None and result.max_drawdown_pct < 0 else None
    return {
        "net_return_pct": result.return_pct,
        "oos_status": "not_untouched_oos",
        "trade_count": len(trades),
        "expectancy_usdt": statistics.mean(trades) if trades else None,
        "profit_factor": gains / losses if losses > 0 else None,
        "win_rate": sum(p > 0 for p in trades) / len(trades) if trades else None,
        "max_drawdown_pct": result.max_drawdown_pct,
        "daily_sharpe": sharpe,
        "daily_sortino": sortino,
        "cagr": cagr,
        "calmar": calmar,
        "largest_trade_contribution_pct_of_initial": 100 * max(trades) / start_equity if trades else None,
        "reliability": "historically viewed data; close-only equity and estimated execution costs",
    }


def _moments(values: list[float]) -> tuple[float, float, float, float]:
    n = len(values)
    mean = statistics.mean(values)
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    sd = math.sqrt(variance)
    skew = sum(((x - mean) / sd) ** 3 for x in values) / n
    kurt = sum(((x - mean) / sd) ** 4 for x in values) / n
    return mean, sd, skew, kurt


def deflated_sharpe(returns: list[float], trial_sharpes: list[float], *, independent_trials: int | None = None) -> dict:
    """Bailey/Lopez de Prado DSR; all Sharpe values are per-observation, unannualized."""
    if len(returns) < 30 or len(trial_sharpes) < 2:
        return {"status": "insufficient_data"}
    mean, sd, skew, kurt = _moments(returns)
    if sd <= 0:
        return {"status": "zero_variance"}
    n = independent_trials or len(trial_sharpes)
    if n < 2:
        return {"status": "insufficient_trials"}
    sigma_trials = statistics.stdev(trial_sharpes)
    gamma = 0.5772156649015329
    normal = NormalDist()
    max_z = (1 - gamma) * normal.inv_cdf(1 - 1 / n) + gamma * normal.inv_cdf(1 - 1 / (n * math.e))
    threshold = sigma_trials * max_z
    sr = mean / sd
    denominator_squared = 1 - skew * sr + (kurt - 1) * sr * sr / 4
    if denominator_squared <= 0:
        return {"status": "invalid_moments"}
    probability = normal.cdf((sr - threshold) * math.sqrt(len(returns) - 1) / math.sqrt(denominator_squared))
    return {"status": "computed", "dsr_probability": probability, "observed_sharpe_per_bar": sr,
            "selection_threshold_per_bar": threshold, "trial_count": len(trial_sharpes),
            "assumed_independent_trials": n, "sample_count": len(returns),
            "warning": "Requires complete trial registry and comparable aligned returns; dependence among trials affects effective count."}


def probability_backtest_overfitting(aligned_returns: dict[str, list[float]], *, partitions: int = 8,
                                     metric: str = "sharpe") -> dict:
    """CSCV/PBO, ranking the IS winner on complementary OOS partitions."""
    if len(aligned_returns) < 3 or partitions < 4 or partitions % 2:
        return {"status": "insufficient_configurations_or_partitions"}
    lengths = {len(v) for v in aligned_returns.values()}
    if len(lengths) != 1 or min(lengths) < partitions * 30:
        return {"status": "unaligned_or_too_short"}
    names = list(aligned_returns)
    length = lengths.pop()
    blocks = [list(range((j * length) // partitions, ((j + 1) * length) // partitions)) for j in range(partitions)]

    def score(values: list[float]) -> float:
        if metric == "total_return":
            total = 1.0
            for value in values:
                total *= 1 + value
            return total - 1
        if metric != "sharpe":
            raise ValueError("metric must be sharpe or total_return")
        if len(values) < 2:
            return float("-inf")
        sd = statistics.stdev(values)
        return statistics.mean(values) / sd if sd > 0 else float("-inf")

    bad = 0
    total = 0
    logits = []
    for subset in itertools.combinations(range(partitions), partitions // 2):
        chosen = set(subset)
        train_idx = [i for b in subset for i in blocks[b]]
        test_idx = [i for b in range(partitions) if b not in chosen for i in blocks[b]]
        ins = {name: score([aligned_returns[name][i] for i in train_idx]) for name in names}
        out = {name: score([aligned_returns[name][i] for i in test_idx]) for name in names}
        winner = max(names, key=lambda name: (ins[name], name))
        ordered = sorted(names, key=lambda name: (out[name], name))
        rank = ordered.index(winner) + 1
        omega = rank / (len(names) + 1)
        logit = math.log(omega / (1 - omega))
        logits.append(logit)
        bad += logit < 0
        total += 1
    return {"status": "computed", "pbo": bad / total, "negative_logit_count": bad,
            "combination_count": total, "configuration_count": len(names),
            "partitions": partitions, "selection_metric": metric, "median_logit": statistics.median(logits),
            "warning": "CSCV diagnostic is not chronological forward OOS; provide all attempted configurations."}
