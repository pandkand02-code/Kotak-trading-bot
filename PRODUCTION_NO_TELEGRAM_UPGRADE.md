# Production No-Telegram Execution Upgrade

This build removes Telegram and Claude dependence from the execution path.

## Core endpoints
- `/signals/advanced` — deterministic RSS + technical + regime scoring. Default min confidence is 60.
- `/chain/affordable` — scans deep option chain and selects the minimum affordable tradable premium based on wallet.
- `/trade/execute` — one-shot guarded execution: signal -> wallet option selection -> risk -> Kotak order.
- `/quotes/option_full` — available for option LTP/OI/volume lookups.

## Execution rules
- No Telegram.
- No browser Claude API key.
- Live order only through `/trade/execute`.
- Limit order with 2% max slippage by default.
- Time guard: no entry before 09:30 and after 14:40 IST unless bypassed.
- Expiry-day blocked unless explicitly allowed.
- Wallet-aware minimum option selection.
- Risk engine still checks wallet, daily loss cap, max trades per day.

## Small wallet behavior
If wallet is low, the selector scans deeper OTM strikes. It chooses the cheapest option that passes `min_option_ltp` and fits inside deployable wallet budget.

## Warning
This is execution-ready code, not a profit guarantee. Test dry-run and one-lot limit orders first.
