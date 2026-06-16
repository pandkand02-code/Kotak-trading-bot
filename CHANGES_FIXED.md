# Fixed build notes

This package is based on the uploaded bot, but patched for the errors visible in your screenshot.

## Main fixes
- Added missing news cache globals in `main.py`: `_news_cache`, `_tg_cache`, `NEWS_TTL`, `TG_TTL`.
- Added missing `_fo_segment()` helper so `/chain/atm` and strike resolution do not crash.
- Added `/chain/resolve_strike` support used by the frontend before placing orders.
- Order placement/cancel now sends Kotak-compatible URL-encoded `jData`.
- Risk settings aligned to your spec: 1% per trade and 5% daily loss stop.
- SENSEX uses `bse_fo`; NIFTY uses `nse_fo`.
- Code compiles with: `python -m py_compile main.py scrip.py risk.py ticks.py streamer.py`.

## Run locally
```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

Open: `http://localhost:8000`

## Safe testing order
1. Login + validate.
2. Check `/wallet/limits`.
3. Check `/quotes/market_data`.
4. Check `/chain/atm`.
5. Use `/orders/test_place` with one lot only. It places a limit order and immediately cancels if accepted.
6. Only after the above, enable auto trade.

## Important
No one can guarantee a trading bot is profitable. This code has guardrails, but live trading still needs paper testing, broker-side validation, and small capital testing first.
