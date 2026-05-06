"""
Kotak NEO scrip master loader.

The scrip master is a daily CSV listing every tradable instrument with its
trading symbol (pTrdSymbol), neo symbol (pSymbol), strike, expiry, and
option type. We need it to map a (NIFTY, 24350, CE, nearest_expiry) request
into the exact pTrdSymbol that /orders/place needs and the pSymbol that
/script-details/.../{symbol}/ltp returns prices for.

Download flow:
  1. GET {base_url}/script-details/1.0/masterscrip/file-paths
     headers: Authorization=<access_token>
     -> {"data": {"filesPaths": ["...nse_fo...csv", ...]}}
  2. Pick the URL containing 'nse_fo' (or 'nse_cm' for equities).
  3. GET that URL (it's a public S3-style URL, no auth needed).

Cache key is the date — we re-download once per trading day.
"""

from __future__ import annotations
import csv
import io
import logging
import time
from datetime import date
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class ScripMaster:
    def __init__(self):
        # cache_key = (segment, date.isoformat()) -> list[dict]
        self._cache: dict[tuple[str, str], list[dict]] = {}
        # filesPaths cache per day
        self._paths_cache: tuple[str, dict] | None = None

    async def _file_paths(self, c: httpx.AsyncClient, sess: dict) -> dict:
        today = date.today().isoformat()
        if self._paths_cache and self._paths_cache[0] == today:
            return self._paths_cache[1]
        url = f"{sess['base_url'].rstrip('/')}/script-details/1.0/masterscrip/file-paths"
        r = await c.get(url, headers={"Authorization": sess.get("access_token", "")})
        try:
            data = r.json()
        except Exception as e:
            raise RuntimeError(f"masterscrip file-paths: non-JSON HTTP {r.status_code}: {r.text[:200]}") from e
        paths_map: dict[str, str] = {}
        files = (data.get("data") or {}).get("filesPaths") or []
        for url in files:
            url_s = str(url)
            if "nse_fo" in url_s:
                paths_map["nse_fo"] = url_s
            elif "nse_cm" in url_s:
                paths_map["nse_cm"] = url_s
            elif "bse_fo" in url_s:
                paths_map["bse_fo"] = url_s
            elif "bse_cm" in url_s:
                paths_map["bse_cm"] = url_s
        if not paths_map:
            raise RuntimeError(f"masterscrip: no segment files in {files[:3]}")
        self._paths_cache = (today, paths_map)
        return paths_map

    async def load(self, sess: dict, segment: str = "nse_fo") -> list[dict]:
        """Return parsed scrip rows for the given segment, downloading once per day."""
        key = (segment, date.today().isoformat())
        if key in self._cache:
            return self._cache[key]

        async with httpx.AsyncClient(timeout=120) as c:
            paths = await self._file_paths(c, sess)
            file_url = paths.get(segment)
            if not file_url:
                raise RuntimeError(f"scrip master: no {segment} file (have {list(paths)})")
            logger.info(f"scrip master: downloading {segment} -> {file_url[:100]}")
            r = await c.get(file_url)
            if r.status_code != 200:
                raise RuntimeError(f"scrip download HTTP {r.status_code}: {r.text[:200]}")
            text = r.text

        # Some Kotak files use trailing semicolons in headers, e.g. "pSymbol;,...".
        # Normalize and parse with csv module.
        text = text.replace(";,", ",").replace(";\n", "\n").replace(";\r", "\r")
        reader = csv.DictReader(io.StringIO(text))
        rows = []
        for row in reader:
            # Strip leading/trailing whitespace from keys and values
            rows.append({(k or "").strip(): (v or "").strip() for k, v in row.items()})
        logger.info(f"scrip master: parsed {len(rows)} rows for {segment}")
        self._cache[key] = rows
        return rows

    async def options_for(
        self,
        sess: dict,
        symbol: str = "NIFTY",
        segment: str = "nse_fo",
    ) -> list[dict]:
        """Return all rows for symbol (e.g. NIFTY) sorted by expiry, strike, option_type."""
        rows = await self.load(sess, segment)
        out = []
        for row in rows:
            sname = row.get("pSymbolName") or row.get("pSymbol") or ""
            otype = row.get("pOptionType") or ""
            if sname.upper() != symbol.upper():
                continue
            if otype not in ("CE", "PE"):
                continue
            try:
                strike = float(row.get("dStrikePrice") or 0) / 100.0  # Kotak stores * 100
            except ValueError:
                continue
            if strike <= 0:
                continue
            out.append({
                "p_symbol":     row.get("pSymbol", ""),
                "p_trd_symbol": row.get("pTrdSymbol", ""),
                "strike":       strike,
                "option_type":  otype,
                "expiry":       row.get("lExpiryDate", ""),
                "lot_size":     int(float(row.get("iLotSize") or row.get("lotSize") or 0) or 0),
            })
        return out

    async def find_atm_chain(
        self,
        sess: dict,
        symbol: str,
        spot: float,
        n: int = 5,
        step: int | None = None,
    ) -> dict:
        """Return ATM±n strikes (CE+PE) for the nearest expiry of `symbol`."""
        opts = await self.options_for(sess, symbol)
        if not opts:
            return {"atm": 0, "expiry": "", "strikes": []}

        # Group by expiry; pick smallest expiry >= today.
        # Expiry format from Kotak is usually a unix-epoch-seconds string or 'DD-MMM-YYYY'.
        def expiry_sort_key(e: str):
            # numeric epoch seconds first
            try:
                return (0, int(e))
            except ValueError:
                pass
            return (1, e)
        expiries = sorted({o["expiry"] for o in opts if o["expiry"]}, key=expiry_sort_key)
        if not expiries:
            return {"atm": 0, "expiry": "", "strikes": []}
        # Pick first expiry whose epoch (if numeric) is >= now
        nearest = expiries[0]
        now_ts = time.time()
        for e in expiries:
            try:
                if int(e) >= now_ts - 86400:  # allow today
                    nearest = e
                    break
            except ValueError:
                nearest = e
                break

        chain_opts = [o for o in opts if o["expiry"] == nearest]
        if step is None:
            step = 100 if symbol.upper() == "BANKNIFTY" else 50
        atm_strike = round(spot / step) * step
        wanted = {atm_strike + (i - n) * step for i in range(2 * n + 1)}

        rows: dict[float, dict] = {}
        for o in chain_opts:
            if o["strike"] not in wanted:
                continue
            row = rows.setdefault(o["strike"], {"strike": o["strike"], "ce": None, "pe": None})
            key = "ce" if o["option_type"] == "CE" else "pe"
            row[key] = {
                "p_symbol":     o["p_symbol"],
                "p_trd_symbol": o["p_trd_symbol"],
                "lot_size":     o["lot_size"],
            }
        return {
            "atm":     atm_strike,
            "expiry":  nearest,
            "step":    step,
            "strikes": sorted(rows.values(), key=lambda r: r["strike"]),
        }
