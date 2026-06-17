# Fresh News + SQLite Memory Upgrade

## What changed

- Removed Telegram/Claude from execution path.
- Added free RSS news collector.
- Added 3-5 minute freshness guard using `NEWS_MAX_AGE_SECONDS`.
- Added automatic SQLite database creation: `news_memory.db`.
- Added `/news/memory` endpoint to inspect stored headlines.
- `/news/sentiment` now returns:
  - `sentiment_score`
  - `bias`
  - `confidence`
  - `impact_score`
  - `fresh_count`
  - `usable_count`
  - scored headline items with reasons
- Old news is stored for memory/audit but ignored for live signals.

## Important reality check

Free RSS is not guaranteed to publish within 3-5 minutes. The bot will enforce the freshness rule. If no fresh news arrives, news contributes neutral score instead of forcing trades.

## Runtime variables

Optional:

```bash
export NEWS_MAX_AGE_SECONDS=300
export NEWS_DB_FILE=news_memory.db
```

## Test endpoints

```bash
curl http://127.0.0.1:8000/news/memory
```

Use the UI or API for `/signals/advanced` and `/trade/execute`.
