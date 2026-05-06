"""
Intraday tick recorder.

Kotak's /script-details/.../ltp returns only the last traded price — no OHLC,
no change %. Without those, the AI signal layer has no real price-action to
reason about and just outputs WAIT/NEUTRAL.

This recorder watches every LTP we fetch and exposes day-stats derived from
the session's tick window:

    open   = first ltp we saw today
    high   = max ltp today
    low    = min ltp today
    change = (current - open) / open * 100   (intraday %)

Also exposes a short rolling window so the AI can see momentum (last vs
5-min-ago LTP) without needing a separate historical-bars endpoint.

Day rollover is automatic: if the gap since the last tick exceeds 4 hours,
the next tick starts a new session. This handles overnight, weekends, and
process restarts cleanly.
"""

from __future__ import annotations
import time
from collections import deque
from threading import RLock


DAY_GAP_SECONDS = 4 * 3600   # >4h since last tick -> roll the day
MAX_TICKS = 5000             # cap per-symbol history
MOMENTUM_WINDOW_SECONDS = 5 * 60  # 5-min momentum reference


class TickRecorder:
    def __init__(self):
        self._lock = RLock()
        # symbol -> deque[(ts, ltp)]
        self._ticks: dict[str, deque] = {}
        # symbol -> (open_ts, open_ltp)
        self._session_open: dict[str, tuple[float, float]] = {}

    def record(self, symbol: str, ltp: float) -> dict:
        """Record a tick and return derived day-stats. Returns an empty dict
        if ltp is non-positive."""
        try:
            ltp = float(ltp)
        except (TypeError, ValueError):
            return {}
        if ltp <= 0:
            return {}

        with self._lock:
            now = time.time()
            dq = self._ticks.get(symbol)
            if dq is None:
                dq = deque(maxlen=MAX_TICKS)
                self._ticks[symbol] = dq

            # Day rollover: if the gap from the last tick is huge, reset.
            if dq:
                last_ts, _ = dq[-1]
                if now - last_ts > DAY_GAP_SECONDS:
                    dq.clear()
                    self._session_open[symbol] = (now, ltp)
            dq.append((now, ltp))

            if symbol not in self._session_open:
                self._session_open[symbol] = (now, ltp)

            open_ts, open_ltp = self._session_open[symbol]
            ltps = [p for _, p in dq]
            high = max(ltps)
            low  = min(ltps)
            change_pct = ((ltp - open_ltp) / open_ltp * 100.0) if open_ltp > 0 else 0.0

            # Momentum: ltp - ltp_5min_ago
            cutoff = now - MOMENTUM_WINDOW_SECONDS
            ref = next((p for t, p in dq if t >= cutoff), open_ltp)
            momentum_pct = ((ltp - ref) / ref * 100.0) if ref > 0 else 0.0

            return {
                "open":          round(open_ltp, 2),
                "high":          round(high, 2),
                "low":           round(low, 2),
                "change":        round(change_pct, 4),
                "momentum_5m":   round(momentum_pct, 4),
                "ticks_count":   len(dq),
                "session_age_s": round(now - open_ts, 1),
            }

    def status(self) -> dict:
        with self._lock:
            return {
                sym: {
                    "ticks":   len(dq),
                    "first":   dq[0] if dq else None,
                    "last":    dq[-1] if dq else None,
                    "session_open": self._session_open.get(sym),
                }
                for sym, dq in self._ticks.items()
            }
