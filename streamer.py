"""
Kotak NEO live-quote WebSocket streamer.

Kotak's REST quote endpoints (/quotes, /quick/quotes) return HTTP 503 "no
available server" on most NEO accounts because live quotes have been moved to
WebSocket-only. This module runs one persistent WS connection per session,
subscribes to instrument tokens, caches the latest tick per token, and exposes
the cache as a plain dict that the HTTP handlers can read synchronously.

WS protocol (HSI):
  url:        wss://mlhsm.kotaksecurities.com/realtime?sId={hsServerId}
  on connect: send  {"Authorization", "Sid", "type": "cn", ...}
  expect:     {"type":"cn","msg":"OK"}  (or similar success ack)
  subscribe:  {"type":"mws","scrips":"nse_cm|26000&nse_cm|26009","channelnum":1}
  ticks:      JSON frames keyed by "tk" (token); fields used: ltp, c, nc, o,h,l, v, oi
"""

from __future__ import annotations
import asyncio
import json
import logging
import time
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

# Kotak NEO HSI (High Speed Index) streaming endpoint.
HSI_HOST = "wss://mlhsm.kotaksecurities.com"


def _coerce_float(v: Any) -> float:
    if v is None or v == "":
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


class KotakStreamer:
    """One streamer instance per logged-in session.

    Public surface:
      await s.ensure_running()           — idempotent, starts the WS task
      await s.subscribe(tokens)          — tokens = [{"instrument_token","exchange_segment"}, ...]
      s.get_tick(token) -> dict|None     — latest tick for a token, or None
      s.status() -> dict                 — connection state + counters
      await s.close()                    — tear down
    """

    def __init__(self, session_token: str, session_sid: str, hs_server_id: str = ""):
        self.session_token = session_token
        self.session_sid = session_sid
        self.hs_server_id = hs_server_id or "server_1"

        self.ticks: dict[str, dict] = {}     # token -> latest tick dict
        self.subscribed: set[str] = set()    # "nse_cm|26000" scrip codes
        self.pending_subs: set[str] = set()  # subs queued before connect

        self._ws: websockets.WebSocketClientProtocol | None = None
        self._task: asyncio.Task | None = None
        self._connected_at: float = 0.0
        self._last_tick_at: float = 0.0
        self._frames_received: int = 0
        self._reconnects: int = 0
        self._last_error: str = ""
        self._stop = False

    # ── lifecycle ────────────────────────────────────────────────────────
    async def ensure_running(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop = False
        self._task = asyncio.create_task(self._run_forever(), name="kotak-streamer")

    async def close(self) -> None:
        self._stop = True
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    # ── subscriptions ────────────────────────────────────────────────────
    async def subscribe(self, tokens: list[dict]) -> None:
        scrips = [f'{t["exchange_segment"]}|{t["instrument_token"]}' for t in tokens]
        new = [s for s in scrips if s not in self.subscribed]
        if not new:
            return
        if self._ws and not self._ws.closed:
            await self._send_subscribe(new)
        else:
            self.pending_subs.update(new)

    async def _send_subscribe(self, scrips: list[str]) -> None:
        msg = {
            "type": "mws",
            "scrips": "&".join(scrips),
            "channelnum": 1,
        }
        try:
            await self._ws.send(json.dumps(msg))
            self.subscribed.update(scrips)
            logger.info(f"[streamer] subscribed: {scrips}")
        except Exception as e:
            self._last_error = f"subscribe failed: {e}"
            logger.error(self._last_error)

    # ── connection loop ──────────────────────────────────────────────────
    async def _run_forever(self) -> None:
        backoff = 1.0
        while not self._stop:
            try:
                await self._connect_and_read()
                backoff = 1.0  # reset after a clean session
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.warning(f"[streamer] connection error: {self._last_error}")
            if self._stop:
                break
            self._reconnects += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def _connect_and_read(self) -> None:
        url = f"{HSI_HOST}/realtime?sId={self.hs_server_id}"
        logger.info(f"[streamer] connecting {url}")
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            self._ws = ws
            self._connected_at = time.time()

            # 1. Auth handshake
            await ws.send(json.dumps({
                "Authorization": self.session_token,
                "Sid": self.session_sid,
                "type": "cn",
                "source": "API",
                "x-server-id": self.hs_server_id,
            }))

            # 2. Replay any subscriptions queued before connect
            if self.pending_subs:
                queued = list(self.pending_subs)
                self.pending_subs.clear()
                await self._send_subscribe(queued)

            # 3. Read frames until closed
            async for raw in ws:
                self._frames_received += 1
                self._last_tick_at = time.time()
                self._handle_frame(raw)
            self._ws = None

    # ── frame handling ───────────────────────────────────────────────────
    def _handle_frame(self, raw: str | bytes) -> None:
        # Some Kotak frames may arrive as bytes; try utf-8 decode then JSON.
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode("utf-8", errors="replace")
            except Exception:
                return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # Binary tick frame on this account variant — log first occurrence.
            if self._frames_received <= 3:
                logger.info(f"[streamer] non-JSON frame ({len(raw)} bytes): {raw[:80]!r}")
            return

        # Kotak sometimes wraps a list of ticks inside a single frame.
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            ftype = item.get("type", "")
            if ftype in ("cn", "ack", "error"):
                logger.info(f"[streamer] control frame: {item}")
                if ftype == "error":
                    self._last_error = str(item)
                continue

            tk = str(item.get("tk") or item.get("instrument_token") or "")
            if not tk:
                continue
            ltp = item.get("ltp", item.get("lp"))
            close = item.get("c", item.get("close"))
            nc = item.get("nc", item.get("change_percentage"))
            self.ticks[tk] = {
                "tk": tk,
                "ltp": _coerce_float(ltp),
                "open": _coerce_float(item.get("o", item.get("open"))),
                "high": _coerce_float(item.get("h", item.get("high"))),
                "low": _coerce_float(item.get("l", item.get("low"))),
                "close": _coerce_float(close),
                "change": _coerce_float(nc),
                "volume": _coerce_float(item.get("v", item.get("volume"))),
                "oi": _coerce_float(item.get("oi", item.get("open_interest"))),
                "ts": self._last_tick_at,
            }

    # ── read API ─────────────────────────────────────────────────────────
    def get_tick(self, instrument_token: str) -> dict | None:
        return self.ticks.get(str(instrument_token))

    def status(self) -> dict:
        return {
            "connected": bool(self._ws and not self._ws.closed),
            "connected_at": self._connected_at,
            "last_tick_at": self._last_tick_at,
            "frames_received": self._frames_received,
            "reconnects": self._reconnects,
            "last_error": self._last_error,
            "subscribed": sorted(self.subscribed),
            "tokens_cached": sorted(self.ticks.keys()),
        }
