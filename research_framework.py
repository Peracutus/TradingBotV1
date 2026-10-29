#!/usr/bin/env python3
"""Daily BTC/ETH research pipeline. Reads local candles; never places orders."""
from __future__ import annotations

import csv
import json
import math
import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
START_CAPITAL = 1000.0
BASE_FEE = 0.001
BASE_SLIP = 0.0005
SUPPORTED = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18,
             20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 32, 42, 43}
DESCRIPTIVE = {30, 44, 45}
EXTERNAL = {19, 33, 34, 35, 36, 37, 38, 39, 40, 41, 46, 47, 48, 49, 50}


def catalog():
    source = (ROOT / "hypotheses_source.txt").read_text()
    matches = list(re.finditer(r"(?m)^\s*(\d+)\.\s+(.+)$", source))
    rows = []
    for k, m in enumerate(matches):
        n = int(m.group(1))
        ending = matches[k + 1].start() if k + 1 < len(matches) else len(source)
        desc = source[m.end():ending].strip()
        if n in SUPPORTED:
            status = "runnable"
            need = ["BTC/ETH daily OHLCV"]
        elif n in DESCRIPTIVE:
            status = "descriptive"
            need = ["BTC/ETH daily OHLCV"]
        elif n == 17:
            status = "not_applicable_to_24h_spot"
            need = ["a market with an actual session gap"]
        elif n in EXTERNAL:
            status = "blocked_missing_data"
            need = (["gold and macro history"] if n >= 46 else
                    ["perpetual funding/open interest/liquidations"] if 35 <= n <= 40 else
                    ["historical order book/trade tape"] if n in (33, 34) else
                    ["additional market/session data"])
        else:
            status = "pending_implementation"
            need = ["BTC/ETH daily OHLCV"]
        rows.append({"id": n, "title": m.group(2).strip(), "description": desc,
                     "status": status, "required_data": need})
    if [r["id"] for r in rows] != list(range(1, 51)):
        raise ValueError("Expected all 50 numbered hypotheses")
    return rows


def load_daily(symbol):
    with (ROOT / f"{symbol}USDT_240.csv").open() as f:
        raw = list(csv.DictReader(f))
    by_date = {}
    for r in raw:
        day = datetime.fromtimestamp(int(r["time"]) / 1000, timezone.utc).date().isoformat()
        by_date.setdefault(day, []).append(r)
    result = []
    for day, bars in sorted(by_date.items()):
        hours = [datetime.fromtimestamp(int(x["time"])/1000,timezone.utc).hour for x in bars]
        if len(bars) != 6 or hours != [0,4,8,12,16,20]:
            continue  # use only complete UTC days
        if any(float(x["low"]) <= 0 or float(x["low"]) > float(x["high"]) or
               not float(x["low"]) <= float(x["open"]) <= float(x["high"]) or
               not float(x["low"]) <= float(x["close"]) <= float(x["high"])
               for x in bars):
            raise ValueError(f"Invalid OHLC on {symbol} {day}")
        result.append({"date": day, "open": float(bars[0]["open"]),
                       "high": max(float(x["high"]) for x in bars),
                       "low": min(float(x["low"]) for x in bars),
                       "close": float(bars[-1]["close"]),
                       "volume": sum(float(x["volume"]) for x in bars)})
    return result


def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def ema(xs, n):
    out = []
    z = xs[0]
    alpha = 2 / (n + 1)
    for x in xs:
        z += alpha * (x - z)
        out.append(z)
    return out


def rsi(closes, n=14):
    out = [50.0] * len(closes)
    for i in range(n, len(closes)):
        ups = sum(max(0.0, closes[j] - closes[j - 1]) for j in range(i-n+1, i+1))
        downs = sum(max(0.0, closes[j - 1] - closes[j]) for j in range(i-n+1, i+1))
        out[i] = 100.0 if downs == 0 and ups else (50.0 if ups == downs == 0
                 else 100 - 100 / (1 + ups / downs))
    return out


def features(rows):
    close = [r["close"] for r in rows]
    high = [r["high"] for r in rows]
    low = [r["low"] for r in rows]
    vol = [r["volume"] for r in rows]
    tr = [high[0] - low[0]]
    for i in range(1, len(rows)):
        tr.append(max(high[i] - low[i], abs(high[i] - close[i-1]),
                      abs(low[i] - close[i-1])))
    atr = ema(tr, 14)
    e20, e50, e200 = ema(close, 20), ema(close, 50), ema(close, 200)
    rs = rsi(close)
    z20, width20, adx, obv = [0.0]*len(rows), [0.0]*len(rows), [0.0]*len(rows), [0.0]*len(rows)
    plus_dm, minus_dm = [0.0]*len(rows), [0.0]*len(rows)
    for i in range(1, len(rows)):
        up = high[i] - high[i-1]
        down = low[i-1] - low[i]
        plus_dm[i] = up if up > down and up > 0 else 0
        minus_dm[i] = down if down > up and down > 0 else 0
        obv[i] = obv[i-1] + (vol[i] if close[i] > close[i-1] else
                             -vol[i] if close[i] < close[i-1] else 0)
    ep, em = ema(plus_dm, 14), ema(minus_dm, 14)
    for i in range(19, len(rows)):
        w = close[i-19:i+1]
        sd = statistics.pstdev(w)
        z20[i] = (close[i]-mean(w))/sd if sd else 0
        width20[i] = 4*sd/mean(w) if mean(w) else 0
        pdi = 100*ep[i]/atr[i] if atr[i] else 0
        mdi = 100*em[i]/atr[i] if atr[i] else 0
        adx[i] = 100*abs(pdi-mdi)/(pdi+mdi) if pdi+mdi else 0
    adx = ema(adx, 14)
    return {"close": close, "high": high, "low": low, "volume": vol,
            "atr": atr, "ema20": e20, "ema50": e50, "ema200": e200,
            "rsi": rs, "z20": z20, "width20": width20, "adx": adx, "obv": obv}


def signal(hid, sym, i, data, feats):
    f = feats[sym]
    c, hi, lo, v = f["close"], f["high"], f["low"], f["volume"]
    atr, e20, e50 = f["atr"], f["ema20"], f["ema50"]
    rsi14, z, adx = f["rsi"], f["z20"], f["adx"]
    breakout20 = c[i] > max(hi[i-20:i])
    breakout55 = c[i] > max(hi[i-55:i])
    cross = e20[i-1] <= e50[i-1] and e20[i] > e50[i]
    volume_avg = mean(v[i-20:i])
    atr_avg = mean(atr[i-50:i])
    width_avg = mean(f["width20"][i-50:i])
    btc = feats["BTC"]["close"]
    btc_move = btc[i]/btc[i-1]-1
    entry = False
    if hid == 1: entry = breakout20
    elif hid == 2: entry = breakout20 and atr[i] > atr_avg
    elif hid == 3: entry = c[i] > c[i-20]
    elif hid == 4: entry = c[i] > c[i-60]
    elif hid == 5: entry = c[i]/c[i-30]-1 > (c[i]/c[i-90]-1)/3
    elif hid == 6: entry = cross
    elif hid == 7: entry = cross and adx[i] > 20
    elif hid == 8: entry = c[i] > mean(c[i-199:i+1])
    elif hid == 9: entry = breakout55
    elif hid == 10: entry = all(c[j] > hi[j-1] for j in range(i-2,i+1))
    elif hid == 11: entry = c[i] < e20[i] - 2*atr[i]
    elif hid == 12: entry = z[i-1] < -2 and z[i] > -2
    elif hid == 13: entry = z[i] < -2
    elif hid == 14: entry = rsi14[i] < 25
    elif hid == 15:
        prior = min(range(i-10,i),key=lambda j:lo[j])
        entry = lo[i] < lo[prior] and rsi14[i] > rsi14[prior] and rsi14[i] < 40
    elif hid == 16: entry = (c[i]/c[i-1]-1) < -2*atr[i]/c[i]
    elif hid == 18:
        entry = atr[i-1] > 1.5*mean(atr[i-51:i-1]) and c[i] > c[i-1]
    elif hid == 20: entry = rsi14[i] < 25 and adx[i] < 20
    elif hid == 21: entry = breakout20 and atr[i-1] < mean(atr[i-50:i])
    elif hid == 22: entry = breakout20 and f["width20"][i-1] < width_avg
    elif hid == 23: entry = breakout20 and atr[i-1] <= sorted(atr[i-50:i])[9]
    elif hid == 24: entry = hi[i-1] < hi[i-2] and lo[i-1] > lo[i-2] and c[i] > hi[i-1]
    elif hid == 25: entry = c[i] > c[i-1] + 1.5*atr[i] and atr[i] > atr_avg
    elif hid == 26: entry = c[i] < c[i-1] - 2*atr[i] and atr[i] > 1.5*atr_avg
    elif hid in (27,28): entry = cross
    elif hid == 29: entry = breakout20 and v[i] > 1.5*volume_avg
    elif hid == 31: entry = c[i] < c[i-1] - atr[i] and v[i] > 2*volume_avg
    elif hid == 32: entry = breakout20 and f["obv"][i] > max(f["obv"][i-20:i])
    elif hid == 42: entry = sym == "ETH" and btc_move > .025 and c[i] > e20[i]
    elif hid == 43: entry = sym == "ETH" and btc[i] > btc[i-20]*1.1 and c[i] > e20[i]
    # These hypotheses are long-only on spot. Short variants need a separate contract dataset.
    if hid in (11,12,13,14,15,16,18,20,26,31):
        exit_now = c[i] >= e20[i] or rsi14[i] >= 55
    elif hid in (1,2,9,21,22,23,24,25,29,32):
        exit_now = c[i] < min(lo[i-10:i])
    elif hid in (42,43):
        exit_now = c[i] < e20[i]
    else:
        exit_now = c[i] < e50[i]
    return entry, exit_now


@dataclass
class Costs:
    fee: float = BASE_FEE
    slip: float = BASE_SLIP
    funding: float = 0.0  # spot only; derivative strategies require a separate feed


def backtest(hid, data, feats, start, end, costs=Costs(), delay=1):
    cash = START_CAPITAL
    pos = {s: None for s in data}
    trades = []
    curve = []
    fees_paid = 0.0
    funding_paid = 0.0
    for t in range(start, end):
        sig_idx = t - delay
        if sig_idx < (200 if hid == 8 else 90):
            continue
        # Daily signal is known only after the prior complete day.
        for sym in ("BTC","ETH"):
            row = data[sym][t]
            p = pos[sym]
            if not p:
                continue
            charge = p["qty"]*row["open"]*costs.funding
            cash -= charge
            funding_paid += charge
            _, exit_signal = signal(hid,sym,sig_idx,data,feats)
            hit_stop = row["low"] <= p["stop"]
            if hit_stop or exit_signal:
                # A signal known before the open executes first; the day's low
                # can stop only a position still held after that open.
                px = (row["open"] if exit_signal else min(row["open"],p["stop"]))*(1-costs.slip)
                proceeds = p["qty"]*px*(1-costs.fee)
                fees_paid += p["qty"]*px*costs.fee
                cash += proceeds
                trades.append({"symbol":sym,"entry":p["entry_date"],"exit":row["date"],
                               "pnl":proceeds-p["cost"]})
                pos[sym] = None
        for sym in ("BTC","ETH"):
            if pos[sym]:
                continue
            enter, _ = signal(hid,sym,sig_idx,data,feats)
            if not enter:
                continue
            row = data[sym][t]
            price = row["open"]*(1+costs.slip)
            atr = feats[sym]["atr"][sig_idx]
            stop = max(0.01,price-2*atr)
            equity = cash+sum(p["qty"]*data[s][t]["open"] for s,p in pos.items() if p)
            remaining_risk = equity*.005-sum(p["qty"]*max(0,p["entry_price"]-p["stop"])
                                            for p in pos.values() if p)
            cap = equity*.35/(price*(1+costs.fee))
            if hid == 28:
                recent = [math.log(feats[sym]["close"][j]/feats[sym]["close"][j-1])
                          for j in range(sig_idx-29,sig_idx+1)]
                realized = statistics.pstdev(recent)*math.sqrt(365)
                cap *= min(1.0,.20/realized) if realized else 1
            elif hid == 27:
                cap *= min(1.0, mean(feats[sym]["atr"][sig_idx-50:sig_idx]) / atr) if atr else 1
            qty = min(max(0,remaining_risk)/(price-stop), cap,
                      cash/(price*(1+costs.fee)))
            if qty*price < 5:
                continue
            cost = qty*price*(1+costs.fee)
            fees_paid += qty*price*costs.fee
            cash -= cost
            pos[sym] = {"qty":qty,"stop":stop,"cost":cost,
                        "entry_price":price,"entry_date":row["date"]}
            # Conservative same-day stop after opening.
            if row["low"] <= stop:
                px = stop*(1-costs.slip)
                proceeds = qty*px*(1-costs.fee)
                fees_paid += qty*px*costs.fee
                cash += proceeds
                trades.append({"symbol":sym,"entry":row["date"],"exit":row["date"],
                               "pnl":proceeds-cost})
                pos[sym] = None
        mark = cash+sum(p["qty"]*data[s][t]["close"]*(1-costs.fee-costs.slip)
                      for s,p in pos.items() if p)
        curve.append(mark)
    for sym,p in pos.items():
        if p:
            px = data[sym][end-1]["close"]*(1-costs.slip)
            proceeds = p["qty"]*px*(1-costs.fee)
            fees_paid += p["qty"]*px*costs.fee
            cash += proceeds
            trades.append({"symbol":sym,"entry":p["entry_date"],
                           "exit":data[sym][end-1]["date"],"pnl":proceeds-p["cost"]})
    peak, mdd = START_CAPITAL, 0.0
    for v in curve:
        peak = max(peak,v)
        mdd = min(mdd,v/peak-1)
    return {"return_pct":round(100*(cash/START_CAPITAL-1),3),
            "max_drawdown_pct":round(100*mdd,3),"trades":len(trades),
            "fees_usdt":round(fees_paid,2),"funding_usdt":round(funding_paid,2),
            "largest_trade_pnl_usdt":round(max((x["pnl"] for x in trades),default=0),2),
            "trade_pnl_usdt":round(sum(x["pnl"] for x in trades),2)}


def descriptive(hid,data,start,end):
    out = {}
    for sym,rows in data.items():
        if hid == 30:
            sample = []
            for i in range(max(20,start),end-5):
                if (rows[i]["close"] > max(rows[j]["high"] for j in range(i-20,i))
                    and rows[i]["volume"] < mean([rows[j]["volume"] for j in range(i-20,i)])):
                    sample.append(rows[i+5]["close"]/rows[i]["close"]-1)
            out[sym] = {"count":len(sample),
                        "mean_next_5d_return_pct":round(100*mean(sample),4)}
            continue
        samples = {}
        for i in range(max(1,start),end):
            # The previous UTC day is the observation whose complete return is known.
            d = datetime.fromisoformat(rows[i]["date"]).weekday()
            key = ("weekend" if d >= 5 else "weekday") if hid == 44 else str(d)
            samples.setdefault(key,[]).append(rows[i]["close"]/rows[i-1]["close"]-1)
        out[sym] = {k:{"count":len(v),"mean_daily_return_pct":round(100*mean(v),4),
                       "daily_vol_pct":round(100*statistics.pstdev(v),4)}
                    for k,v in samples.items()}
    return out


def walk_forward_selection(data, feats, folds):
    """Choose only from earlier 120-day train windows; then score the next block."""
    choices = []
    for fold in folds:
        train_start = max(90, fold["start"] - 120)
        candidates = []
        for hid in sorted(SUPPORTED - {8}):
            train = backtest(hid, data, feats, train_start, fold["start"])
            if train["trades"] >= 3:
                score = train["return_pct"] + 0.5*train["max_drawdown_pct"]
                candidates.append((score,hid,train))
        if not candidates or max(candidates)[0] <= 0:
            choices.append({"test_dates":[fold["first_date"],fold["last_date"]],
                            "chosen_id":None,"reason":"no positive train score after drawdown penalty",
                            "test":{"return_pct":0.0,"trades":0}})
            continue
        score,hid,train = max(candidates)
        test = backtest(hid,data,feats,fold["start"],fold["end"])
        choices.append({"train_dates":[data["BTC"][train_start]["date"],
                                       data["BTC"][fold["start"]-1]["date"]],
                        "test_dates":[fold["first_date"],fold["last_date"]],
                        "chosen_id":hid,"train_score":round(score,3),
                        "train":train,"test":test})
    compounded = math.prod(1 + x["test"]["return_pct"]/100 for x in choices) - 1
    return {"folds":choices,"compounded_test_return_pct":round(100*compounded,3),
            "note":"independent cash reset at each fold; selected only from earlier dates"}


def main():
    cat = catalog()
    (ROOT/"hypotheses_50.json").write_text(json.dumps(cat,ensure_ascii=False,indent=2))
    raw = {s:load_daily(s) for s in ("BTC","ETH")}
    common = sorted(set(x["date"] for x in raw["BTC"]) & set(x["date"] for x in raw["ETH"]))
    data = {s:[r for r in raw[s] if r["date"] in set(common)] for s in raw}
    feats = {s:features(data[s]) for s in data}
    n = len(common)
    if n < 360:
        raise ValueError("Need at least 360 common daily bars for this walk-forward layout")
    # Four chronological non-overlapping evaluation blocks; fixed rule parameters.
    boundaries = [201, 261, 321, 381, n]
    folds = [{"start":boundaries[k],"end":boundaries[k+1],
              "first_date":common[boundaries[k]],"last_date":common[boundaries[k+1]-1]}
             for k in range(len(boundaries)-1)]
    results = []
    for h in cat:
        hid = h["id"]
        if h["status"] == "runnable":
            blocks = [backtest(hid,data,feats,f["start"],f["end"]) for f in folds]
            full = backtest(hid,data,feats,201,n)
            annual = backtest(hid,data,feats,n-365,n) if hid != 8 else None
            robust = {"double_costs":backtest(hid,data,feats,201,n,
                       Costs(fee=BASE_FEE*2,slip=BASE_SLIP*2)),
                      "two_day_execution_delay":backtest(hid,data,feats,201,n,delay=2)}
            results.append({"id":hid,"title":h["title"],"folds":blocks,
                            "full_period_diagnostic":full,
                            "annual_diagnostic":annual,"robustness":robust})
        elif h["status"] == "descriptive":
            results.append({"id":hid,"title":h["title"],"descriptive":descriptive(hid,data,201,n)})
    output = {"data":{"symbols":["BTCUSDT","ETHUSDT"],"source":"local Bybit spot 4h candles aggregated to complete UTC days",
                      "first_date":common[0],"last_date":common[-1],"daily_bars":n},
              "method":{"initial_capital_usdt":START_CAPITAL,
                        "fee_each_side":BASE_FEE,"slippage_each_side":BASE_SLIP,
                        "funding":"zero for spot; derivative hypotheses blocked",
                        "shared_open_risk_cap_pct":.5,"asset_cap_pct":35,
                        "signal":"closed UTC day; next-day open execution",
                        "folds":folds,
                        "annual_window":{"first_date":common[n-365],"last_date":common[-1],
                                         "note":"ID 8 requires 200-day SMA warmup and is unavailable for a full 365-day window"},
                        "oos_caveat":"historical dates were inspected in earlier research; no pristine unseen holdout"},
              "catalog_counts":{s:sum(h["status"]==s for h in cat)
                                for s in sorted(set(h["status"] for h in cat))},
              "results":results,
              "out_of_sample_diagnostic":{"dates":[folds[-1]["first_date"],folds[-1]["last_date"]],
                                          "qualification":"historically exposed in the earlier project; exploratory only"},
              "walk_forward_selection":walk_forward_selection(data,feats,folds)}
    (ROOT/"framework_results.json").write_text(json.dumps(output,ensure_ascii=False,indent=2))
    with (ROOT/"framework_summary.csv").open("w") as f:
        w=csv.writer(f)
        w.writerow(["id","hypothesis","common_298d_return_pct","common_298d_mdd_pct",
                    "trades","fold_returns_pct","double_cost_return_pct",
                    "delay_2d_return_pct","annual_return_pct","annual_mdd_pct","annual_trades"])
        for r in results:
            if "full_period_diagnostic" not in r: continue
            x=r["full_period_diagnostic"]
            annual = r["annual_diagnostic"]
            w.writerow([r["id"],r["title"],x["return_pct"],x["max_drawdown_pct"],
                        x["trades"],";".join(str(z["return_pct"]) for z in r["folds"]),
                        r["robustness"]["double_costs"]["return_pct"],
                        r["robustness"]["two_day_execution_delay"]["return_pct"],
                        annual["return_pct"] if annual else "",
                        annual["max_drawdown_pct"] if annual else "",
                        annual["trades"] if annual else ""])
    ranked=sorted((r for r in results if "full_period_diagnostic" in r),
                  key=lambda r:r["full_period_diagnostic"]["return_pct"],reverse=True)
    print(json.dumps({"data":output["data"],"catalog_counts":output["catalog_counts"],
                      "folds":folds,
                      "walk_forward_selection":output["walk_forward_selection"],
                      "top_10":[{"id":r["id"],
                        "return_pct":r["full_period_diagnostic"]["return_pct"],
                        "mdd_pct":r["full_period_diagnostic"]["max_drawdown_pct"],
                        "trades":r["full_period_diagnostic"]["trades"],
                        "folds":[z["return_pct"] for z in r["folds"]]}
                        for r in ranked[:10]]},ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
