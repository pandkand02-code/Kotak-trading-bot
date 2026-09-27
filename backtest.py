#!/usr/bin/env python3
"""
Standalone backtest for the NIFTY options bot's signal logic.

WHAT THIS IS: ports the exact technical/pivot/regime scoring and composite-score
formula from main.py (as of this writing) and replays it against REAL historical
NIFTY index and VIX data pulled from Yahoo Finance, using the same 60%-confidence
threshold, 3.5%/3% SL/TP, and post-open buffer as the live bot.

WHAT THIS IS NOT: a realistic execution simulation. Option premiums here are
SYNTHESIZED via Black-Scholes from the historical index price + historical VIX
(as an implied-vol proxy) — not real historical option bid/ask data, which isn't
freely available. This means:
  - No real bid-ask spread (a major cause of the instant-SL-hit trades seen live)
  - No freeze-quantity capital-stranding effect
  - No real order-fill slippage
So the win rate this reports will likely be MORE OPTIMISTIC than real trading.
Treat this as answering "does the signal logic have directional edge in principle",
not "would this have made money after real execution costs".

USAGE:
    pip install httpx --break-system-packages   # if not already installed
    python3 backtest.py                          # defaults: NIFTY, 60 days, 15m bars
    python3 backtest.py --instrument NIFTY --days 60 --interval 15m

Run this ON THE DROPLET (not in a sandbox) since it needs working network access
to Yahoo Finance — confirmed working from your server's IP throughout your bot's
own live logs (yahoo_ok=True), which is why this isn't run from a sandbox instead.
"""

import argparse
import math
import time
from datetime import datetime, timedelta, timezone

import httpx

IST = timezone(timedelta(hours=5, minutes=30))

_YAHOO_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

_YAHOO_SYM = {
    "NIFTY": "^NSEI",
    "SENSEX": "^BSESN",
    "VIX": "^INDIAVIX",
}

# ---------------------------------------------------------------------------
# Ported verbatim (or near-verbatim) from main.py, so this backtest reflects
# the ACTUAL live scoring logic, not a reimplementation that could drift from it.
# ---------------------------------------------------------------------------

def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


def _ema(values, period):
    if not values:
        return None
    if len(values) < period:
        return sum(values) / len(values)
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for price in values[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def _rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))
    gains = gains[-period:]
    losses = losses[-period:]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _macd(values):
    if len(values) < 26:
        return {"macd": None, "signal": None, "histogram": None}
    macd_line = (_ema(values, 12) or 0) - (_ema(values, 26) or 0)
    macd_series = []
    for i in range(26, len(values) + 1):
        sub = values[:i]
        macd_series.append((_ema(sub, 12) or 0) - (_ema(sub, 26) or 0))
    signal = _ema(macd_series, 9) if macd_series else None
    hist = macd_line - signal if signal is not None else None
    return {"macd": macd_line, "signal": signal, "histogram": hist}


def _pivot_levels(h, l, c):
    pp = (h + l + c) / 3
    r1 = 2 * pp - l
    s1 = 2 * pp - h
    r2 = pp + (h - l)
    s2 = pp - (h - l)
    r3 = h + 2 * (pp - l)
    s3 = l - 2 * (h - pp)
    return {"pivot": pp, "r1": r1, "r2": r2, "r3": r3, "s1": s1, "s2": s2, "s3": s3}


def technical_from_bar(ltp, open_, high, low, vix, change, momentum_5m, prices, pivots):
    """Faithful port of main.py's _technical_from_market(), plus one addition:
    pivot_score is now tracked SEPARATELY from the rest of the technical score
    (not just summed in), so a multi-factor confirmation check can ask 'does the
    pivot signal agree with the core technical signal' as an independent vote,
    rather than only ever seeing them pre-blended into one number."""
    ema20 = _ema(prices, 20) if prices else None
    ema50 = _ema(prices, 50) if prices else None
    MIN_TICKS_FOR_NOISY_SIGNALS = 20
    have_enough_data = len(prices) >= MIN_TICKS_FOR_NOISY_SIGNALS
    rsi14 = _rsi(prices, 14) if (prices and have_enough_data) else None
    macd = _macd(prices) if prices else {"macd": None, "signal": None, "histogram": None}
    day_range = max(high - low, 0.01)
    vwap_proxy = (high + low + ltp) / 3 if ltp else 0

    core_score = 0.0
    if ltp and ema20:
        core_score += 0.18 if ltp > ema20 else -0.18
    if ema20 and ema50:
        core_score += 0.16 if ema20 > ema50 else -0.16
    if rsi14 is not None:
        if 55 <= rsi14 <= 70:
            core_score += 0.16
        elif 30 <= rsi14 <= 45:
            core_score -= 0.16
        elif rsi14 > 78:
            core_score -= 0.10
        elif rsi14 < 22:
            core_score += 0.10
    if macd.get("histogram") is not None:
        core_score += 0.14 if macd["histogram"] > 0 else -0.14
    if ltp > vwap_proxy:
        core_score += 0.10
    elif ltp:
        core_score -= 0.10
    if have_enough_data:
        if ltp >= high - day_range * 0.06 and change > 0:
            core_score += 0.14
        if ltp <= low + day_range * 0.06 and change < 0:
            core_score -= 0.14
        if momentum_5m > 0.20:
            core_score += 0.10
        elif momentum_5m < -0.20:
            core_score -= 0.10

    pivot_score = 0.0
    if pivots and ltp:
        levels = sorted([pivots["s3"], pivots["s2"], pivots["s1"], pivots["pivot"],
                          pivots["r1"], pivots["r2"], pivots["r3"]])
        resistance_above = min([lv for lv in levels if lv > ltp], default=None)
        support_below    = max([lv for lv in levels if lv < ltp], default=None)
        tolerance = max(day_range * 0.10, ltp * 0.0015) if ltp else 0
        near_resistance  = resistance_above is not None and (resistance_above - ltp) <= tolerance
        near_support     = support_below    is not None and (ltp - support_below) <= tolerance
        broke_resistance = resistance_above is not None and ltp > resistance_above + tolerance * 0.3
        broke_support    = support_below    is not None and ltp < support_below - tolerance * 0.3
        if near_resistance and not broke_resistance:
            pivot_score -= 0.12
        if near_support and not broke_support:
            pivot_score += 0.12
        if have_enough_data:
            if broke_resistance and momentum_5m > 0:
                pivot_score += 0.12
            if broke_support and momentum_5m < 0:
                pivot_score -= 0.12

    score = _clamp(core_score + pivot_score, -1.0, 1.0)
    data_quality = min(100, 25 + len(prices) * 3)
    confidence = int(_clamp(abs(score) * 100 * 0.75 + data_quality * 0.25, 0, 100))
    return {"technical_score": score, "confidence": confidence,
            "core_score": core_score, "pivot_score": pivot_score}


def market_regime_score(vix, change):
    """Faithful port of main.py's _market_regime_score() — high VIX pushes the
    score bearish (-0.25) at user's explicit request; low VIX has no
    directional effect (that asymmetry is intentional)."""
    score = 0.0
    confidence_penalty = 0.0
    if vix >= 22:
        score -= 0.25
    if abs(change) >= 0.35:
        score += 0.10 if change > 0 else -0.10
    return _clamp(score, -1, 1), confidence_penalty


def composite_signal(technical, regime_score, confidence_penalty):
    """Faithful port of the current no-news composite formula (0.85/0.15 split,
    matching main.py's have_news=False path) with the VIX confidence-penalty fix."""
    composite = technical["technical_score"] * 0.85 + regime_score * 0.15
    confidence = int(_clamp(
        technical["confidence"] * 0.85 + abs(regime_score) * 100 * 0.15 + 10 - confidence_penalty,
        0, 100
    ))
    composite = _clamp(composite, -1.0, 1.0)
    if confidence < 60:
        action = "WAIT"
    elif composite > 0.22:
        action = "BUY_CE"
    elif composite < -0.22:
        action = "BUY_PE"
    else:
        action = "WAIT"
    return action, composite, confidence


def composite_signal_multifactor(technical, regime_score, confidence_penalty, vote_threshold=0.15):
    """EXPERIMENTAL: same composite/confidence gate as composite_signal(), plus a
    second, independent requirement — at least 2 of 3 sub-signals (core technical,
    pivot S/R, market regime) must agree in direction with the composite. Signals
    that are essentially flat (near zero) abstain rather than vote either way, and
    if fewer than 2 signals have a real opinion at all, the trade is skipped
    (not enough independent confirmation to trust it either way).

    This is a genuinely different filter from just raising the confidence
    threshold: it asks whether independent pieces of evidence AGREE, not just
    whether one blended number is large. That's the actual hypothesis being
    tested here, and 60 days of real data will show whether it holds up."""
    action, composite, confidence = composite_signal(technical, regime_score, confidence_penalty)
    if action == "WAIT":
        return action, composite, confidence, {"votes_for": 0, "votes_total": 0}

    composite_sign = 1 if composite > 0 else -1
    votes = []
    core = technical.get("core_score", 0)
    if abs(core) >= vote_threshold:
        votes.append(1 if core > 0 else -1)
    pivot = technical.get("pivot_score", 0)
    if abs(pivot) >= 0.01:  # pivot contributions are discrete (0, ±0.12, ±0.24) — any nonzero counts
        votes.append(1 if pivot > 0 else -1)
    if abs(regime_score) >= 0.01:
        votes.append(1 if regime_score > 0 else -1)

    agreeing = sum(1 for v in votes if v == composite_sign)
    vote_info = {"votes_for": agreeing, "votes_total": len(votes), "votes_raw": votes}
    if len(votes) < 2 or agreeing < 2:
        return "WAIT", composite, confidence, vote_info
    return action, composite, confidence, vote_info


def bs_price(S, K, T, sigma, r, opt_type):
    """Port of bot.html's bs() Black-Scholes pricer."""
    if T <= 0 or sigma <= 0:
        return max(0.0, S - K if opt_type == "CE" else K - S)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    def N(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))
    if opt_type == "CE":
        return max(0.0, S * N(d1) - K * math.exp(-r * T) * N(d2))
    return max(0.0, K * math.exp(-r * T) * N(-d2) - S * N(-d1))


def nearest_strike(spot, step=50):
    return round(spot / step) * step


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_yahoo_intraday(symbol, days_back, interval):
    sym = _YAHOO_SYM[symbol]
    sym_enc = sym.replace("^", "%5E")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym_enc}?interval={interval}&range={days_back}d"
    r = httpx.get(url, headers={"User-Agent": _YAHOO_UA, "Accept": "application/json"}, timeout=20)
    r.raise_for_status()
    data = r.json()
    result = data["chart"]["result"][0]
    ts = result["timestamp"]
    quote = result["indicators"]["quote"][0]
    bars = []
    for i in range(len(ts)):
        if quote["close"][i] is None:
            continue
        bars.append({
            "t": ts[i],
            "open": quote["open"][i], "high": quote["high"][i],
            "low": quote["low"][i], "close": quote["close"][i],
        })
    return bars


def fetch_yahoo_period(symbol, period1_ts, period2_ts, interval):
    """Same as fetch_yahoo_intraday but bounded to an exact [period1, period2]
    unix-timestamp window instead of 'last N days from now' — needed to target
    one specific historical date precisely for the hourly report."""
    sym = _YAHOO_SYM[symbol]
    sym_enc = sym.replace("^", "%5E")
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym_enc}"
           f"?interval={interval}&period1={period1_ts}&period2={period2_ts}")
    r = httpx.get(url, headers={"User-Agent": _YAHOO_UA, "Accept": "application/json"}, timeout=20)
    r.raise_for_status()
    data = r.json()
    result = data["chart"]["result"][0]
    ts = result["timestamp"]
    quote = result["indicators"]["quote"][0]
    bars = []
    for i in range(len(ts)):
        if quote["close"][i] is None:
            continue
        bars.append({
            "t": ts[i],
            "open": quote["open"][i], "high": quote["high"][i],
            "low": quote["low"][i], "close": quote["close"][i],
        })
    return bars


def fetch_yahoo_daily(symbol, days_back):
    return fetch_yahoo_intraday(symbol, days_back + 5, "1d")


# ---------------------------------------------------------------------------
# Single-day hourly report: for one historical trading day, buckets the session
# into hourly windows (09:15-10:15, 10:15-11:15, ... aligned to NSE hours) and
# reports, per hour: VIX momentum, index high/low/direction, and what the bot's
# actual technical/composite scoring would have read at that hour's close.
# Writes a CSV ("separate sheet") alongside printing a readable table.
# ---------------------------------------------------------------------------

def hourly_report(date_str, instrument="NIFTY", interval="5m", out_csv=None):
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    day_start_dt = datetime(target_date.year, target_date.month, target_date.day, 9, 15, tzinfo=IST)
    day_end_dt   = datetime(target_date.year, target_date.month, target_date.day, 15, 30, tzinfo=IST)
    # Pad the fetch window by a day on each side — Yahoo's period1/period2 are
    # UTC-based and intraday history windows can be finicky right at boundaries.
    p1 = int((day_start_dt - timedelta(days=1)).timestamp())
    p2 = int((day_end_dt + timedelta(days=1)).timestamp())

    print(f"Fetching {instrument} and VIX intraday ({interval}) for {date_str}...")
    idx_bars = [b for b in fetch_yahoo_period(instrument, p1, p2, interval)
                if datetime.fromtimestamp(b["t"], IST).date() == target_date]
    vix_bars = [b for b in fetch_yahoo_period("VIX", p1, p2, interval)
                if datetime.fromtimestamp(b["t"], IST).date() == target_date]
    print(f"  {instrument}: {len(idx_bars)} bars | VIX: {len(vix_bars)} bars")
    if not idx_bars:
        print(f"No {instrument} bars found for {date_str} — check it was a trading "
              f"day and is within Yahoo's intraday history window (~60 days back).")
        return

    # Previous day's H/L/C for pivots (best-effort; not fatal if unavailable)
    prev_window_start = int((day_start_dt - timedelta(days=6)).timestamp())
    daily_bars = fetch_yahoo_period(instrument, prev_window_start, p1, "1d")
    pivots = None
    if daily_bars:
        prev_bar = daily_bars[-1]
        pivots = _pivot_levels(prev_bar["high"], prev_bar["low"], prev_bar["close"])

    vix_by_ts = {b["t"]: b["close"] for b in vix_bars}

    def vix_at_or_before(ts):
        candidates = [v for t, v in vix_by_ts.items() if t <= ts]
        return candidates[-1] if candidates else None

    hours = []
    cursor = day_start_dt
    while cursor < day_end_dt:
        hours.append((cursor, min(cursor + timedelta(hours=1), day_end_dt)))
        cursor += timedelta(hours=1)

    prices_so_far = []
    rows = []
    for h_start, h_end in hours:
        h_start_ts, h_end_ts = h_start.timestamp(), h_end.timestamp()
        bucket = [b for b in idx_bars if h_start_ts <= b["t"] < h_end_ts]
        if not bucket:
            continue
        bucket_high = max(b["high"] for b in bucket)
        bucket_low = min(b["low"] for b in bucket)
        bucket_open = bucket[0]["open"]
        bucket_close = bucket[-1]["close"]
        idx_change_pct = (bucket_close - bucket_open) / bucket_open * 100 if bucket_open else 0

        vix_start = vix_at_or_before(bucket[0]["t"]) or 15.0
        vix_end = vix_at_or_before(bucket[-1]["t"]) or vix_start
        vix_momentum_pct = (vix_end - vix_start) / vix_start * 100 if vix_start else 0

        prices_so_far.extend([b["close"] for b in bucket])
        day_open_so_far = idx_bars[0]["open"]
        day_high_so_far = max(b["high"] for b in idx_bars if b["t"] <= bucket[-1]["t"])
        day_low_so_far = min(b["low"] for b in idx_bars if b["t"] <= bucket[-1]["t"])
        day_change_pct = (bucket_close - day_open_so_far) / day_open_so_far * 100 if day_open_so_far else 0
        prev_close_for_mom = prices_so_far[-2] if len(prices_so_far) >= 2 else bucket_close
        momentum_5m = (bucket_close - prev_close_for_mom) / prev_close_for_mom * 100 if prev_close_for_mom else 0

        technical = technical_from_bar(bucket_close, day_open_so_far, day_high_so_far, day_low_so_far,
                                        vix_end, day_change_pct, momentum_5m, prices_so_far[-120:], pivots)
        regime_score, conf_penalty = market_regime_score(vix_end, day_change_pct)
        action, composite, confidence = composite_signal(technical, regime_score, conf_penalty)

        rows.append({
            "hour": f"{h_start.strftime('%H:%M')}-{h_end.strftime('%H:%M')}",
            "nifty_open": round(bucket_open, 2), "nifty_close": round(bucket_close, 2),
            "nifty_high": round(bucket_high, 2), "nifty_low": round(bucket_low, 2),
            "nifty_change_pct": round(idx_change_pct, 3),
            "vix_start": round(vix_start, 2), "vix_end": round(vix_end, 2),
            "vix_momentum_pct": round(vix_momentum_pct, 3),
            "technical_score": round(technical["technical_score"], 3),
            "confidence": confidence, "composite": composite, "lean": action,
        })

    print(f"\n{'='*100}")
    print(f"HOURLY REPORT — {instrument} — {date_str}")
    print(f"{'='*100}")
    header = f"{'Hour':>11} | {'Nifty O':>9} {'Close':>9} {'High':>9} {'Low':>9} {'Chg%':>7} | {'VIX Start':>9} {'VIX End':>7} {'VIXmom%':>8} | {'Tech':>6} {'Conf':>4} {'Lean':>7}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(f"{r['hour']:>11} | {r['nifty_open']:>9} {r['nifty_close']:>9} {r['nifty_high']:>9} {r['nifty_low']:>9} "
              f"{r['nifty_change_pct']:>+6.2f}% | {r['vix_start']:>9} {r['vix_end']:>7} {r['vix_momentum_pct']:>+7.2f}% | "
              f"{r['technical_score']:>+5.2f} {r['confidence']:>4} {r['lean']:>7}")

    if out_csv is None:
        out_csv = f"hourly_report_{instrument}_{date_str}.csv"
    import csv as _csv
    with open(out_csv, "w", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        w.writeheader()
        w.writerows(rows)
    print(f"\nSaved to {out_csv} — open in Excel/Google Sheets.")


# ---------------------------------------------------------------------------
# Backtest engine
# ---------------------------------------------------------------------------

def run_backtest(instrument="NIFTY", days_back=60, interval="15m",
                  min_confidence=60, sl_pct=3.5, tp_pct=3.0,
                  entry_open_min=9 * 60 + 40, entry_close_min=15 * 60 + 15,
                  multifactor=False):
    print(f"Fetching {days_back} days of {interval} bars for {instrument}...")
    bars = fetch_yahoo_intraday(instrument, days_back, interval)
    print(f"  got {len(bars)} bars")

    interval_minutes = {"5m": 5, "15m": 15, "30m": 30, "60m": 60, "1h": 60}.get(interval, 15)
    bar_years = interval_minutes / (60 * 24 * 365)
    cooldown_bars = max(1, math.ceil(3 / interval_minutes))

    print(f"Fetching {days_back} days of daily VIX + {instrument} for prev-day pivots...")
    vix_daily = fetch_yahoo_daily("VIX", days_back)
    idx_daily = fetch_yahoo_daily(instrument, days_back)
    vix_by_date = {}
    for b in vix_daily:
        d = datetime.fromtimestamp(b["t"], IST).date()
        vix_by_date[d] = b["close"]
    daily_by_date = {}
    for b in idx_daily:
        d = datetime.fromtimestamp(b["t"], IST).date()
        daily_by_date[d] = b

    sorted_dates = sorted(daily_by_date.keys())
    prev_day_map = {}
    for i, d in enumerate(sorted_dates):
        if i == 0:
            continue
        prev_day_map[d] = daily_by_date[sorted_dates[i - 1]]

    trades = []
    prices_by_day = {}
    day_open_high_low = {}
    cooldown_until = 0
    open_trade = None

    for i, bar in enumerate(bars):
        dt = datetime.fromtimestamp(bar["t"], IST)
        d = dt.date()
        mins = dt.hour * 60 + dt.minute
        ltp = bar["close"]

        prices_by_day.setdefault(d, []).append(ltp)
        oh = day_open_high_low.setdefault(d, {"open": bar["open"], "high": bar["high"], "low": bar["low"]})
        oh["high"] = max(oh["high"], bar["high"])
        oh["low"] = min(oh["low"], bar["low"])

        # Manage an open synthetic trade first — re-price the option via Black-
        # Scholes at this bar's index level/remaining time/current-day VIX, and
        # compare the repriced premium against the TP/SL premium targets set at
        # entry (this is more realistic than tracking an index-level proxy,
        # since options don't move linearly with the underlying).
        if open_trade:
            bars_elapsed = i - open_trade["entry_i"]
            remaining_dte = max(0.0001, open_trade["dte"] - bars_elapsed * bar_years)
            cur_vix = vix_by_date.get(d, open_trade["iv"] * 100) / 100
            cur_premium = bs_price(ltp, open_trade["strike"], remaining_dte, cur_vix, 0.06, open_trade["side"])
            hit_tp = cur_premium >= open_trade["tp_premium"]
            hit_sl = cur_premium <= open_trade["sl_premium"]
            if hit_tp or hit_sl or remaining_dte <= 0.0001:
                result = "WIN" if hit_tp and not hit_sl else "LOSS" if hit_sl else ("WIN" if cur_premium >= open_trade["entry_premium"] else "LOSS")
                pnl_pct = (cur_premium - open_trade["entry_premium"]) / open_trade["entry_premium"] * 100
                trades.append({**open_trade, "exit_premium": cur_premium, "result": result,
                               "pnl_pct": pnl_pct, "exit_time": dt.isoformat()})
                open_trade = None
                cooldown_until = i + cooldown_bars

        if open_trade or i < cooldown_until:
            continue
        if mins < entry_open_min or mins >= entry_close_min:
            continue
        if dt.weekday() >= 5:
            continue

        prev = prev_day_map.get(d)
        pivots = _pivot_levels(prev["high"], prev["low"], prev["close"]) if prev else None
        vix = vix_by_date.get(d, 15.0)
        open_ = oh["open"]
        change = (ltp - open_) / open_ * 100 if open_ else 0
        prev_close = prices_by_day[d][-2] if len(prices_by_day[d]) >= 2 else ltp
        momentum_5m = (ltp - prev_close) / prev_close * 100 if prev_close else 0
        prices_window = prices_by_day[d][-120:]

        technical = technical_from_bar(ltp, open_, oh["high"], oh["low"], vix, change, momentum_5m, prices_window, pivots)
        regime_score, confidence_penalty = market_regime_score(vix, change)
        if multifactor:
            action, composite, confidence, votes = composite_signal_multifactor(technical, regime_score, confidence_penalty)
        else:
            action, composite, confidence = composite_signal(technical, regime_score, confidence_penalty)

        if action == "WAIT" or confidence < min_confidence:
            continue

        side = "CE" if action == "BUY_CE" else "PE"
        strike = nearest_strike(ltp)
        # Matches the Tuesday weekly-expiry assumption already used elsewhere in
        # this bot's code (_isExpiryDay in bot.html) — NSE has changed Nifty's
        # weekly expiry day before, so treat this as approximate too.
        days_to_tue = (1 - dt.weekday()) % 7
        dte_days = max(0.5, days_to_tue)
        dte = dte_days / 365
        iv = vix / 100
        entry_premium = bs_price(ltp, strike, dte, iv, 0.06, side)
        if entry_premium < 0.5:
            continue
        tp_target = entry_premium * (1 + tp_pct / 100)
        sl_target = entry_premium * (1 - sl_pct / 100)
        open_trade = {
            "entry_time": dt.isoformat(), "side": side, "strike": strike, "dte": dte, "iv": iv,
            "entry_index": ltp, "entry_premium": entry_premium,
            "tp_premium": tp_target, "sl_premium": sl_target,
            "confidence": confidence, "composite": composite, "entry_i": i,
        }

    print(f"\n{'='*60}")
    print(f"BACKTEST RESULTS — {instrument}, {days_back} days, {interval} bars"
          f"{' [MULTI-FACTOR MODE]' if multifactor else ' [STANDARD MODE]'}")
    print(f"{'='*60}")
    print("⚠ SYNTHETIC OPTION PRICING (Black-Scholes from index+VIX) — no real")
    print("  bid-ask spread, freeze-qty, or slippage. Likely more optimistic")
    print("  than real trading. See docstring at top of this file.\n")
    if not trades:
        print("No trades fired — try more days, a different interval, or check")
        print("that the data actually returned (see bar/day counts above).")
        return
    wins = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    win_rate = len(wins) / len(trades) * 100
    avg_pnl = sum(t["pnl_pct"] for t in trades) / len(trades)
    ce_trades = [t for t in trades if t["side"] == "CE"]
    pe_trades = [t for t in trades if t["side"] == "PE"]
    print(f"Total simulated trades : {len(trades)}")
    print(f"Wins / Losses          : {len(wins)} / {len(losses)}")
    print(f"Win probability        : {win_rate:.1f}%")
    print(f"Avg P&L per trade      : {avg_pnl:+.2f}% (of premium)")
    print(f"CE trades / PE trades  : {len(ce_trades)} / {len(pe_trades)}")
    print(f"\nFirst 5 trades:")
    for t in trades[:5]:
        print(f"  {t['entry_time']} {t['side']} strike={t['strike']} "
              f"entry=₹{t['entry_premium']:.2f} exit=₹{t['exit_premium']:.2f} {t['result']}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--instrument", default="NIFTY")
    p.add_argument("--days", type=int, default=60)
    p.add_argument("--interval", default="15m")
    p.add_argument("--min-confidence", type=int, default=60,
                    help="Confidence threshold (0-100). Try 70/75/80 to see the raise-the-bar lever.")
    p.add_argument("--multifactor", action="store_true",
                    help="EXPERIMENTAL: require 2-of-3 independent signals (core technical, pivot, regime) to agree, on top of the normal confidence gate.")
    p.add_argument("--single-day", default=None,
                    help="YYYY-MM-DD: run the hourly report for one specific day instead of the multi-day trade backtest")
    args = p.parse_args()
    if args.single_day:
        hourly_report(args.single_day, instrument=args.instrument, interval=args.interval)
    else:
        run_backtest(instrument=args.instrument, days_back=args.days, interval=args.interval,
                     min_confidence=args.min_confidence, multifactor=args.multifactor)
