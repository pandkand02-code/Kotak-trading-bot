"""
Risk engine for the trading bot.

Holds the canonical wallet balance + daily PnL counters and tells callers
whether a new entry is allowed under the user's caps:

  - Per-trade  : TP +3%, SL -2%   (defaults; per-trade override allowed)
  - Daily      : +10% lock-in profit, -8% lock-in loss  (kill-switch)
  - Overall    : -10% wallet drawdown                    (hard kill-switch)

All percentages are relative to the day-start wallet snapshot, so the caps
are deterministic even as the wallet drifts during the session. The
day-start snapshot resets when the calendar day changes (handled in
ensure_day_rolled()).
"""

from __future__ import annotations
import logging, time
from dataclasses import dataclass, field
from datetime import date

logger = logging.getLogger(__name__)


@dataclass
class RiskConfig:
    daily_profit_pct:  float = 10.0  # optional lock-in at +10%
    daily_loss_pct:    float = 5.0   # user spec: stop trading at -5%
    overall_loss_pct:  float = 5.0   # hard stop aligned to daily loss cap
    per_trade_tp_pct:  float = 3.0   # default take profit
    per_trade_sl_pct:  float = 2.0   # default stop loss
    capital_per_trade_pct: float = 1.0  # user spec: max 1% capital at risk/deployed per trade
    # Spec-mandated trade-frequency limits.
    max_trades_per_day:      int = 10   # hard ceiling
    consec_loss_pause_count: int = 3    # pause trading after this many losses in a row
    consec_loss_pause_min:   int = 30   # auto-resume after this many minutes


@dataclass
class RiskState:
    wallet:          float = 0.0   # current available margin (live, refreshed from /wallet/limits)
    day_start:       float = 0.0   # wallet snapshot at start of trading day
    day_start_date:  str = ""      # YYYY-MM-DD when day_start was captured
    realised_pnl:    float = 0.0   # booked PnL for the day
    trades_today:    int = 0
    wins_today:      int = 0
    losses_today:    int = 0
    consec_losses:   int = 0       # current streak; resets on any win
    pause_until_ts:  float = 0.0   # epoch-seconds; if > now, entries blocked (cooldown)
    pause_reason:    str = ""
    halted:          bool = False  # True when a cap fires
    halted_reason:   str = ""
    config: RiskConfig = field(default_factory=RiskConfig)


class RiskEngine:
    def __init__(self, config: RiskConfig | None = None):
        self.s = RiskState(config=config or RiskConfig())

    # ── lifecycle ────────────────────────────────────────────────────────
    def set_wallet(self, available_margin: float) -> None:
        """Called whenever /wallet/limits succeeds. Captures the day-start
        snapshot the first time we see a balance for the calendar day."""
        try:
            available_margin = float(available_margin)
        except (TypeError, ValueError):
            return
        self.s.wallet = available_margin
        self.ensure_day_rolled()
        if self.s.day_start <= 0 and available_margin > 0:
            # First balance of the day -> snapshot
            self.s.day_start = available_margin
            self.s.day_start_date = date.today().isoformat()
            logger.info(f"[risk] day_start snapshot = ₹{available_margin:.2f}")

    def ensure_day_rolled(self) -> None:
        today = date.today().isoformat()
        if self.s.day_start_date and self.s.day_start_date != today:
            logger.info(f"[risk] new day {today} (was {self.s.day_start_date}) — resetting counters")
            self.s.day_start = self.s.wallet
            self.s.day_start_date = today
            self.s.realised_pnl = 0.0
            self.s.trades_today = 0
            self.s.wins_today = 0
            self.s.losses_today = 0
            self.s.halted = False
            self.s.halted_reason = ""

    # ── booking ──────────────────────────────────────────────────────────
    def book_trade(self, pnl: float) -> None:
        """Record a closed trade. Updates realised PnL, wallet (best-effort —
        the truth still lives in Kotak's avlMrgn which the frontend re-fetches),
        and re-evaluates kill-switches."""
        try:
            pnl = float(pnl)
        except (TypeError, ValueError):
            return
        self.ensure_day_rolled()
        self.s.realised_pnl += pnl
        self.s.wallet += pnl
        self.s.trades_today += 1
        if pnl > 0:
            self.s.wins_today += 1
            self.s.consec_losses = 0  # any win resets the streak
        elif pnl < 0:
            self.s.losses_today += 1
            self.s.consec_losses += 1
            cfg = self.s.config
            if self.s.consec_losses >= cfg.consec_loss_pause_count:
                self.s.pause_until_ts = time.time() + cfg.consec_loss_pause_min * 60
                self.s.pause_reason = (
                    f"{self.s.consec_losses} losses in a row — "
                    f"pause {cfg.consec_loss_pause_min}m to reassess"
                )
                logger.warning(f"[risk] consec-loss pause: {self.s.pause_reason}")
        self._evaluate_caps()

    def _evaluate_caps(self) -> None:
        """Set halted=True with a reason if any cap is breached."""
        cfg = self.s.config
        if self.s.day_start <= 0:
            return
        pct = (self.s.realised_pnl / self.s.day_start) * 100.0
        if pct >= cfg.daily_profit_pct:
            self.s.halted = True
            self.s.halted_reason = f"daily profit cap hit (+{pct:.2f}% ≥ +{cfg.daily_profit_pct}%) — locking in"
            return
        if pct <= -cfg.daily_loss_pct:
            self.s.halted = True
            self.s.halted_reason = f"daily loss cap hit ({pct:.2f}% ≤ -{cfg.daily_loss_pct}%)"
            return
        # Overall drawdown is also evaluated against day_start as a baseline
        # (a full wallet history is overkill for a same-day options bot).
        if pct <= -cfg.overall_loss_pct:
            self.s.halted = True
            self.s.halted_reason = f"overall loss cap hit ({pct:.2f}% ≤ -{cfg.overall_loss_pct}%)"
            return

    # ── pre-trade checks ─────────────────────────────────────────────────
    def can_trade(self, required_margin: float = 0.0) -> tuple[bool, str]:
        """Returns (allowed, reason). reason is empty when allowed."""
        self.ensure_day_rolled()
        self._evaluate_caps()
        if self.s.halted:
            return False, self.s.halted_reason
        if self.s.wallet <= 0:
            return False, f"wallet is ₹{self.s.wallet:.2f} — cannot place new trades"
        # Max-trades-per-day ceiling (spec: 10).
        cfg = self.s.config
        if self.s.trades_today >= cfg.max_trades_per_day:
            return False, (
                f"daily trade cap hit ({self.s.trades_today}/"
                f"{cfg.max_trades_per_day}) — pausing till midnight"
            )
        # Consecutive-loss pause window. Auto-clears when ts passes.
        now = time.time()
        if self.s.pause_until_ts and now < self.s.pause_until_ts:
            remain = int(self.s.pause_until_ts - now)
            return False, f"{self.s.pause_reason} ({remain}s left)"
        if self.s.pause_until_ts and now >= self.s.pause_until_ts:
            # Pause expired — clear it and let the trader try again.
            self.s.pause_until_ts = 0.0
            self.s.pause_reason = ""
            self.s.consec_losses = 0
        if required_margin > 0 and required_margin > self.s.wallet:
            return False, (
                f"insufficient margin: trade needs ₹{required_margin:.2f}, "
                f"wallet has ₹{self.s.wallet:.2f}"
            )
        return True, ""

    def position_size(self, ltp: float, lot_size: int) -> dict:
        """Compute a wallet-aware lot count for a single options trade.

        Caps the deployed capital at config.capital_per_trade_pct of the
        current wallet. Returns the recommended lots, the resulting qty,
        and the cost.
        """
        cfg = self.s.config
        if ltp <= 0 or lot_size <= 0:
            return {"lots": 0, "qty": 0, "cost": 0.0, "reason": "invalid ltp/lot_size"}
        per_lot_cost = ltp * lot_size
        budget = self.s.wallet * (cfg.capital_per_trade_pct / 100.0)
        lots = max(0, int(budget // per_lot_cost))
        if lots == 0 and self.s.wallet >= per_lot_cost:
            lots = 1  # always allow at least 1 lot if it fits
        return {
            "lots":          lots,
            "qty":           lots * lot_size,
            "cost":          lots * per_lot_cost,
            "budget":        budget,
            "per_lot_cost":  per_lot_cost,
        }

    # ── reporting ────────────────────────────────────────────────────────
    def state(self) -> dict:
        s = self.s
        cfg = s.config
        if s.day_start > 0:
            pct = (s.realised_pnl / s.day_start) * 100.0
            tp_target = s.day_start * (cfg.daily_profit_pct / 100.0)
            sl_target = -s.day_start * (cfg.daily_loss_pct / 100.0)
        else:
            pct = 0.0
            tp_target = sl_target = 0.0
        return {
            "wallet":            s.wallet,
            "day_start":         s.day_start,
            "day_start_date":    s.day_start_date,
            "realised_pnl":      s.realised_pnl,
            "realised_pnl_pct":  pct,
            "trades_today":      s.trades_today,
            "wins_today":        s.wins_today,
            "losses_today":      s.losses_today,
            "halted":            s.halted,
            "halted_reason":     s.halted_reason,
            "caps": {
                "daily_profit_pct":      cfg.daily_profit_pct,
                "daily_loss_pct":        cfg.daily_loss_pct,
                "overall_loss_pct":      cfg.overall_loss_pct,
                "per_trade_tp_pct":      cfg.per_trade_tp_pct,
                "per_trade_sl_pct":      cfg.per_trade_sl_pct,
                "capital_per_trade_pct": cfg.capital_per_trade_pct,
            },
            "targets": {
                "daily_profit_inr": tp_target,
                "daily_loss_inr":   sl_target,
            },
        }

    def reset_day(self) -> None:
        """Manual reset (e.g. user clicks 'Start Fresh Day')."""
        self.s.day_start = self.s.wallet
        self.s.day_start_date = date.today().isoformat()
        self.s.realised_pnl = 0.0
        self.s.trades_today = 0
        self.s.wins_today = 0
        self.s.losses_today = 0
        self.s.halted = False
        self.s.halted_reason = ""
        logger.info("[risk] manual day reset")
