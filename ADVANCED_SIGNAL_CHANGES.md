# Advanced News + Technical Signal Upgrade

## Files changed

### main.py
Added:
- `/signals/advanced`
- `/news/advanced`
- Weighted decision engine:
  - Technical analysis: 60%
  - News sentiment: 20%
  - Market regime: 10%
  - VIX condition: 10%
- Advanced headline scoring with phrase-level sentiment, high-impact news weighting, confidence score, and coverage score.
- Technical scoring using EMA20, EMA50, RSI14, MACD, VWAP proxy, day high/low breakout proxy, 5-minute momentum, and VIX.
- Risk-engine gating before final trade action.

### ticks.py
Added:
- `prices(symbol, limit)` method to expose recent LTP history for indicator calculation.

## New API usage

POST `/signals/advanced`

```json
{
  "session_id": "sess_xxxxxxxx",
  "instrument": "NIFTY",
  "telegram_channel": "",
  "include_telegram": false,
  "min_confidence": 75
}
```

Possible actions:
- `BUY_CE`
- `BUY_PE`
- `WAIT`
- `BLOCKED_BY_RISK`

## Important
This does not guarantee profit. It only improves decision quality by requiring technical confirmation, sentiment confirmation, market regime filtering, and risk validation.
