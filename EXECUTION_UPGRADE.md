# Execution Upgrade

Updated for wallet-aware options execution.

## Added backend endpoints
- `POST /quotes/option_full` - fixes UI 404 and returns LTP/OI/volume for option legs.
- `POST /chain/affordable` - scans option chain and selects nearest affordable CE/PE based on wallet.
- `POST /trade/execute` - full pre-trade flow: time guard, advanced signal, affordable chain, risk check, dry-run/live order.

## Changed execution logic
- `/orders/test_place` no longer blindly selects ATM CE.
- It now selects a wallet-affordable strike from the option chain.
- It places a low-limit order and cancels immediately to test broker connectivity.

## Guards included
- No entry before 09:30 IST.
- No entry after 14:40 IST.
- Expiry-day block by default.
- Risk engine validation before live execution.
- Market order blocked unless explicitly allowed.
- Dry run is default for `/trade/execute`.

## UI updates
- Analyze button now calls `/signals/advanced` after Kotak login.
- Sentiment score, technical score, composite score, and confidence are displayed in logs/signal card.
