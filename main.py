"""
NEXUS Trading Bot v3 — FastAPI Backend
Real Kotak NEO API: Live quotes, OHLC, wallet, orders
Rate limits enforced as per Kotak NEO API documentation:
  - Max 10 requests per second
  - Max 200 orders per minute
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
import os
import httpx, json, asyncio, time
from datetime import datetime
from collections import deque
from pydantic import BaseModel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def safe_json(r: httpx.Response):
    """Parse a Kotak response defensively.

    Returns (data, error). When the upstream returns an empty body or HTML
    error page, r.json() raises json.JSONDecodeError("Expecting value: line 1
    column 1 (char 0)") which is useless to the caller. This helper turns that
    into a structured error containing the HTTP status and a body snippet so
    the frontend can show what Kotak actually replied with.
    """
    text = (r.text or "").strip()
    ctype = r.headers.get("content-type", "")
    if not text:
        return None, f"empty body (HTTP {r.status_code})"
    try:
        return r.json(), None
    except (json.JSONDecodeError, ValueError):
        snippet = text[:300].replace("\n", " ").replace("\r", " ")
        return None, f"non-JSON body (HTTP {r.status_code}, content-type={ctype}): {snippet}"

app = FastAPI(title="NEXUS Trading Bot v3")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ── RATE LIMITER ──────────────────────────────────────────────────────────────
# Kotak NEO API limits (from documentation):
#   General API  : 10 requests / second
#   Order API    : 200 orders  / minute

class RateLimiter:
    def __init__(self):
        # General: 10 req/sec → track timestamps in a 1-second sliding window
        self.general_window  = deque()   # timestamps of general API calls
        self.general_limit   = 10        # max per second
        self.general_period  = 1.0       # seconds

        # Orders: 200 orders/min → track in a 60-second sliding window
        self.order_window    = deque()   # timestamps of order API calls
        self.order_limit     = 200       # max per minute
        self.order_period    = 60.0      # seconds

        # Stats
        self.total_blocked   = 0
        self.total_allowed   = 0

    def _clean(self, dq: deque, period: float) -> None:
        """Remove timestamps older than the window period."""
        cutoff = time.monotonic() - period
        while dq and dq[0] < cutoff:
            dq.popleft()

    def check_general(self) -> bool:
        """Returns True if request is allowed, False if rate limit exceeded."""
        now = time.monotonic()
        self._clean(self.general_window, self.general_period)
        if len(self.general_window) >= self.general_limit:
            self.total_blocked += 1
            logger.warning(f"[RATE LIMIT] General API limit hit: {len(self.general_window)}/{self.general_limit} req/sec")
            return False
        self.general_window.append(now)
        self.total_allowed += 1
        return True

    def check_order(self) -> bool:
        """Returns True if order is allowed, False if order rate limit exceeded."""
        now = time.monotonic()
        self._clean(self.order_window, self.order_period)
        if len(self.order_window) >= self.order_limit:
            self.total_blocked += 1
            logger.warning(f"[RATE LIMIT] Order limit hit: {len(self.order_window)}/{self.order_limit} orders/min")
            return False
        self.order_window.append(now)
        return True

    def status(self) -> dict:
        now = time.monotonic()
        self._clean(self.general_window, self.general_period)
        self._clean(self.order_window, self.order_period)
        gen_used  = len(self.general_window)
        ord_used  = len(self.order_window)
        return {
            "general_api":  {"used": gen_used,  "limit": self.general_limit,  "period": "1s",  "remaining": self.general_limit - gen_used},
            "order_api":    {"used": ord_used,   "limit": self.order_limit,    "period": "60s", "remaining": self.order_limit - ord_used},
            "total_allowed": self.total_allowed,
            "total_blocked": self.total_blocked,
        }

    async def wait_for_slot(self):
        """Wait until a general API slot is available (max wait 1 sec)."""
        waited = 0
        while not self.check_general():
            await asyncio.sleep(0.1)
            waited += 1
            if waited > 10:  # give up after 1 second
                raise HTTPException(
                    status_code=429,
                    detail="Kotak API rate limit: 10 req/sec exceeded. Please wait 1 second."
                )

rl = RateLimiter()

# ── RATE LIMIT STATUS ENDPOINT ────────────────────────────────────────────────
@app.get("/rate_limit/status")
async def rate_limit_status():
    return rl.status()

# ── MIDDLEWARE: auto-apply rate limit to all /auth, /wallet, /quotes, /orders, /positions, /trades ──
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    path = request.url.path
    # Apply rate limiting to all Kotak API proxy endpoints
    protected = ["/auth/", "/wallet/", "/quotes/", "/orders/", "/positions", "/trades", "/chain/", "/scrip/"]
    if any(path.startswith(p) for p in protected):
        if not rl.check_general():
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate limit exceeded",
                    "detail": "Kotak API limit: 10 requests/second. Wait and retry.",
                    "retry_after_seconds": 1,
                    "status": rl.status()
                }
            )
    response = await call_next(request)
    # Add rate limit headers to every response
    s = rl.status()
    response.headers["X-RateLimit-General-Used"]      = str(s["general_api"]["used"])
    response.headers["X-RateLimit-General-Remaining"]  = str(s["general_api"]["remaining"])
    response.headers["X-RateLimit-Orders-Used"]        = str(s["order_api"]["used"])
    response.headers["X-RateLimit-Orders-Remaining"]   = str(s["order_api"]["remaining"])
    return response

KOTAK_LOGIN_URL    = "https://mis.kotaksecurities.com/login/1.0/tradeApiLogin"
KOTAK_VALIDATE_URL = "https://mis.kotaksecurities.com/login/1.0/tradeApiValidate"
NEO_FIN_KEY        = "neotradeapi"
sessions: dict     = {}

# Persist sessions to disk so they survive Railway redeploys. Kotak access
# tokens are valid for ~24h, so persistence means the user only re-logs in
# when their Kotak token actually expires, not every time we push code.
# Path is configurable via SESSIONS_FILE env var; defaults to ./sessions.json.
# If a Railway volume is mounted at /data, point SESSIONS_FILE=/data/sessions.json
# in the service config and sessions survive across deploys too.
SESSIONS_FILE = os.environ.get("SESSIONS_FILE", "sessions.json")
SESSION_TTL_HOURS = int(os.environ.get("SESSION_TTL_HOURS", "20"))


def _sessions_save() -> None:
    try:
        with open(SESSIONS_FILE, "w") as f:
            json.dump(sessions, f)
    except OSError as e:
        logger.warning(f"sessions persist failed: {e}")


def _sessions_load() -> None:
    if not os.path.exists(SESSIONS_FILE):
        return
    try:
        with open(SESSIONS_FILE) as f:
            loaded = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"sessions load failed: {e}")
        return
    cutoff = datetime.now().timestamp() - SESSION_TTL_HOURS * 3600
    kept = 0
    for sid, sess in loaded.items():
        try:
            ts = datetime.fromisoformat(sess.get("created_at", "")).timestamp()
        except ValueError:
            continue
        if ts >= cutoff:
            sessions[sid] = sess
            kept += 1
    logger.info(f"sessions loaded: {kept}/{len(loaded)} from {SESSIONS_FILE}")


@app.on_event("startup")
async def _on_startup():
    _sessions_load()


# Live-quote WebSocket streamers (one per session_id). Kept as a fallback
# path; the primary live-quote source is /script-details/1.0/quotes/neosymbol
# REST (see _ltp_via_script_details).
from streamer import KotakStreamer
streamers: dict[str, KotakStreamer] = {}


async def get_streamer(sid: str) -> KotakStreamer:
    sess = get_session(sid)
    s = streamers.get(sid)
    if s is None:
        s = KotakStreamer(
            session_token=sess["session_token"],
            session_sid=sess["session_sid"],
            hs_server_id=sess.get("hsServerId", ""),
        )
        streamers[sid] = s
    await s.ensure_running()
    return s

# ── Kotak Instrument Tokens (official) ──────────────────────────────────────
# neo_symbol is the human name Kotak's /script-details/1.0/quotes/neosymbol
# REST endpoint expects (e.g. "Nifty 50", "India VIX"). instrument_token is
# kept for reference / WebSocket subscription paths.
INSTRUMENT_TOKENS = {
    "NIFTY":       {"instrument_token": "26000", "exchange_segment": "nse_cm", "neo_symbol": "Nifty 50"},
    "BANKNIFTY":   {"instrument_token": "26009", "exchange_segment": "nse_cm", "neo_symbol": "Nifty Bank"},
    "FINNIFTY":    {"instrument_token": "26037", "exchange_segment": "nse_cm", "neo_symbol": "Nifty Fin Service"},
    "MIDCPNIFTY":  {"instrument_token": "26074", "exchange_segment": "nse_cm", "neo_symbol": "NIFTY MID SELECT"},
    "SENSEX":      {"instrument_token": "1",     "exchange_segment": "bse_cm", "neo_symbol": "SENSEX"},
    "VIX":         {"instrument_token": "26017", "exchange_segment": "nse_cm", "neo_symbol": "India VIX"},
}

# ── Models ────────────────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    access_token: str; mobile: str; ucc: str; totp: str

class ValidateRequest(BaseModel):
    access_token: str; view_token: str; view_sid: str; mpin: str

class SessionRequest(BaseModel):
    session_id: str

class QuoteRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"

class OHLCRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    interval: str = "1"

class OrderRequest(BaseModel):
    session_id: str; trading_symbol: str; transaction_type: str; quantity: str
    order_type: str = "MKT"; price: str = "0"; product: str = "NRML"
    validity: str = "DAY"; exchange_segment: str = "nse_fo"
    amo: str = "NO"; disclosed_quantity: str = "0"
    market_protection: str = "0"; pf: str = "N"; trigger_price: str = "0"

class CancelRequest(BaseModel):
    session_id: str; order_no: str; am: str = "NO"

class SearchRequest(BaseModel):
    session_id: str
    symbol: str; exchange_segment: str = "nse_fo"
    expiry: str = ""; option_type: str = ""; strike_price: str = ""

def get_session(sid):
    if sid not in sessions:
        raise HTTPException(status_code=401, detail="Session expired. Login again.")
    return sessions[sid]

# ── Serve Frontend ────────────────────────────────────────────────────────────
BOT_HTML = open("bot.html").read() if os.path.exists("bot.html") else "<h1>bot.html missing</h1>"

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    return HTMLResponse(content=BOT_HTML)

@app.get("/health")
async def health():
    return {"status": "NEXUS v3 Running", "version": "3.0.0", "time": datetime.now().isoformat()}

# ── AUTH ──────────────────────────────────────────────────────────────────────
@app.post("/auth/login")
async def login(req: LoginRequest):
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(KOTAK_LOGIN_URL,
                headers={"Authorization": req.access_token, "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/json"},
                json={"mobileNumber": req.mobile, "ucc": req.ucc, "totp": req.totp})
            d, err = safe_json(r)
            if err:
                return {"success": False, "message": f"Kotak login: {err}"}
            if d.get("data", {}).get("token"):
                return {"success": True, "view_token": d["data"]["token"], "view_sid": d["data"]["sid"]}
            return {"success": False, "message": d.get("message", "Login failed")}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/auth/validate")
async def validate(req: ValidateRequest):
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(KOTAK_VALIDATE_URL,
                headers={"Authorization": req.access_token, "neo-fin-key": NEO_FIN_KEY,
                         "sid": req.view_sid, "Auth": req.view_token, "Content-Type": "application/json"},
                json={"mpin": req.mpin})
            d, err = safe_json(r)
            if err:
                return {"success": False, "message": f"Kotak validate: {err}"}
            if d.get("data", {}).get("token"):
                sid = f"sess_{req.view_sid[-8:]}"
                sessions[sid] = {
                    "access_token":  req.access_token,   # required for /script-details/* REST quotes
                    "session_token": d["data"]["token"],
                    "session_sid":   d["data"]["sid"],
                    "server_id":     d["data"].get("sid", ""),
                    "base_url":      d["data"].get("baseUrl", "https://cis.kotaksecurities.com"),
                    "hsServerId":    d["data"].get("hsServerId", ""),
                    "created_at":    datetime.now().isoformat()
                }
                _sessions_save()
                return {"success": True, "session_id": sid}
            return {"success": False, "message": d.get("message", "MPIN failed")}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ── WALLET / LIMITS ───────────────────────────────────────────────────────────
@app.post("/wallet/limits")
async def limits(req: SessionRequest):
    """Return Kotak limits flattened so the UI can read avlCash/avlMrgn/etc.
    at the top level. Kotak wraps fields under 'data' or 'Data' depending on
    the endpoint variant — we unwrap whichever is present."""
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(f"{sess['base_url']}/quick/user/limits",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"],
                         "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/x-www-form-urlencoded"},
                data={"jData": json.dumps({"seg": "ALL", "exch": "ALL", "prod": "ALL"})})
            raw, err = safe_json(r)
            if err:
                return {"success": False, "error": err}
            # Unwrap: Kotak returns either {"data": {...fields...}} or
            # {"Data": [{...fields...}]} or sometimes the fields at top level.
            flat: dict = {}
            if isinstance(raw, dict):
                inner = raw.get("data") or raw.get("Data")
                if isinstance(inner, list) and inner:
                    inner = inner[0]
                if isinstance(inner, dict):
                    flat.update(inner)
                # Also surface top-level fields (e.g. when API returns flat)
                for k, v in raw.items():
                    if k not in ("data", "Data") and not isinstance(v, (dict, list)):
                        flat.setdefault(k, v)
            logger.info(f"Wallet limits flattened: avlCash={flat.get('avlCash')!r} keys={list(flat.keys())[:8]}")
            return {"success": True, **flat, "_raw": raw}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ── LIVE QUOTES (LTP + OHLC) ─────────────────────────────────────────────────
@app.post("/quotes/ltp")
async def get_ltp(req: QuoteRequest):
    sess = get_session(req.session_id)
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            payload = {
                "instrument_tokens": [inst],
                "quote_type": "ltp",
                "isIndex": True
            }
            r = await c.post(
                f"{sess['base_url']}/quick/quotes",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"],
                         "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/json"},
                json=payload)
            data, err = safe_json(r)
            if err:
                return {"success": False, "instrument": req.instrument, "error": err}
            logger.info(f"LTP response for {req.instrument}: {data}")
            return {"success": True, "instrument": req.instrument, "data": data}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/quotes/ohlc")
async def get_ohlc(req: QuoteRequest):
    sess = get_session(req.session_id)
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            payload = {
                "instrument_tokens": [inst],
                "quote_type": "ohlc",
                "isIndex": True
            }
            r = await c.post(
                f"{sess['base_url']}/quick/quotes",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"],
                         "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/json"},
                json=payload)
            data, err = safe_json(r)
            if err:
                return {"success": False, "instrument": req.instrument, "error": err}
            logger.info(f"OHLC response for {req.instrument}: {data}")
            return {"success": True, "instrument": req.instrument, "data": data}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ── LIVE MARKET DATA (LTP + VIX via /script-details/1.0/quotes/neosymbol) ───
async def _ltp_via_script_details(c: httpx.AsyncClient, sess: dict, neo_symbol: str, exch: str):
    """Hit Kotak's /script-details/1.0/quotes/neosymbol/{exch}|{symbol}/ltp.

    This is the REST quote path that actually works on NEO accounts (the
    /quotes and /quick/quotes paths return 503). Auth uses the user's static
    access_token, NOT the post-login session_token — that's the trick.

    Returns (ltp_float|None, error_str|None).
    """
    base = sess["base_url"].rstrip("/")
    # neosymbol is "{exch}|{name}" e.g. "nse_cm|Nifty 50"
    url = f"{base}/script-details/1.0/quotes/neosymbol/{exch}|{neo_symbol}/ltp"
    headers = {
        "Authorization": sess.get("access_token", ""),
        "Content-Type": "application/json",
    }
    try:
        r = await c.get(url, headers=headers)
    except httpx.HTTPError as e:
        return None, f"transport: {type(e).__name__}: {e}"
    data, err = safe_json(r)
    if err:
        return None, err
    # Response shape: [{"ltp": "24351.18", ...}]  (sometimes a single dict)
    if isinstance(data, list) and data:
        item = data[0]
    elif isinstance(data, dict) and ("ltp" in data or "data" in data):
        item = data.get("data", data)
        if isinstance(item, list) and item:
            item = item[0]
    else:
        return None, f"unexpected shape: {str(data)[:200]}"
    if not isinstance(item, dict):
        return None, f"unexpected item shape: {str(item)[:200]}"
    try:
        return float(item.get("ltp") or 0), None
    except (TypeError, ValueError) as e:
        return None, f"ltp parse: {e}"


@app.post("/quotes/market_data")
async def get_market_data(req: QuoteRequest):
    """Live LTP + VIX via Kotak's /script-details quotes REST endpoint.

    Falls back to the WebSocket streamer (streamer.py) if REST fails — that
    path is kept in case the script-details endpoint goes 503 on some account
    variants.
    """
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    vix  = INSTRUMENT_TOKENS["VIX"]
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")

    sess = get_session(req.session_id)

    async with httpx.AsyncClient(timeout=15) as c:
        ltp_inst, err_inst = await _ltp_via_script_details(c, sess, inst["neo_symbol"], inst["exchange_segment"])
        ltp_vix,  err_vix  = await _ltp_via_script_details(c, sess, vix["neo_symbol"],  vix["exchange_segment"])

    if ltp_inst is not None and ltp_inst > 0:
        return {
            "success":    True,
            "instrument": req.instrument,
            "ltp":        ltp_inst,
            "vix":        ltp_vix or 0,
            # Change/OHLC require a separate /ohlc call we don't have yet;
            # the UI tolerates 0 here and just shows spot.
            "change": 0, "open": 0, "high": 0, "low": 0, "close": 0,
            "source": "script-details/ltp",
        }

    # REST failed — fall back to the WebSocket streamer (kept around for this).
    try:
        s = await get_streamer(req.session_id)
        await s.subscribe([inst, vix])
        t = s.get_tick(inst["instrument_token"])
        v = s.get_tick(vix["instrument_token"])
        if t and t["ltp"] > 0:
            return {
                "success": True, "instrument": req.instrument,
                "ltp": t["ltp"], "vix": (v or {}).get("ltp", 0),
                "open": t["open"], "high": t["high"], "low": t["low"], "close": t["close"],
                "change": t["change"], "volume": t["volume"], "oi": t["oi"],
                "source": "websocket",
            }
        ws_status = s.status()
    except Exception as e:
        ws_status = {"error": f"{type(e).__name__}: {e}"}

    return {
        "success": False,
        "instrument": req.instrument,
        "ltp": 0, "vix": 0, "change": 0,
        "open": 0, "high": 0, "low": 0, "close": 0,
        "error": err_inst or "no quote source returned data",
        "rest_error_vix": err_vix,
        "ws": ws_status,
    }


@app.post("/quotes/stream_status")
async def stream_status(req: SessionRequest):
    """Inspect the WebSocket streamer for a session — connection state,
    frames received, last error, subscribed scrips, cached tokens."""
    s = streamers.get(req.session_id)
    if s is None:
        return {"running": False, "message": "no streamer for this session yet"}
    return {"running": True, **s.status()}

# ── SEARCH SCRIP (find option chain tokens) ───────────────────────────────────
@app.post("/scrip/search")
async def search_scrip(req: SearchRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            params = {"symbol": req.symbol, "exchange_segment": req.exchange_segment}
            if req.expiry:        params["expiry"] = req.expiry
            if req.option_type:   params["option_type"] = req.option_type
            if req.strike_price:  params["strike_price"] = req.strike_price
            r = await c.get(
                f"{sess['base_url']}/quick/scrips/search",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY},
                params=params)
            data, err = safe_json(r)
            if err:
                return {"success": False, "error": err}
            return data
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ── OPTION CHAIN QUOTES ───────────────────────────────────────────────────────
class ChainRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    spot: float = 24000
    expiry: str = ""

@app.post("/chain/quotes")
async def chain_quotes(req: ChainRequest):
    """Fetch real option chain LTP from Kotak for ATM ±5 strikes"""
    sess = get_session(req.session_id)
    step   = 100 if req.instrument == "BANKNIFTY" else 50
    atm    = round(req.spot / step) * step
    strikes = [atm + (i - 5) * step for i in range(11)]

    # Build instrument tokens for CE and PE
    tokens = []
    inst_map = {}
    for strike in strikes:
        for otype in ["CE", "PE"]:
            # Kotak uses instrument_token from scrip master; we use search API
            key = f"{req.instrument}{strike}{otype}"
            tokens.append({"symbol": req.instrument, "strike": strike, "otype": otype, "key": key})

    # Try to search and get tokens, then fetch quotes
    async with httpx.AsyncClient(timeout=20) as c:
        try:
            # Search for ATM strike to get token format
            search_r = await c.get(
                f"{sess['base_url']}/quick/scrips/search",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY},
                params={"symbol": req.instrument, "exchange_segment": "nse_fo",
                        "expiry": req.expiry, "option_type": "CE", "strike_price": str(atm)})
            search_data, err = safe_json(search_r)
            if err:
                return {"success": False, "error": err, "atm": atm, "strikes": strikes}
            logger.info(f"Scrip search: {str(search_data)[:200]}")
            return {"success": True, "atm": atm, "strikes": strikes, "search_sample": search_data}
        except Exception as e:
            logger.error(f"Chain quotes error: {e}")
            return {"success": False, "error": str(e), "atm": atm, "strikes": strikes}

# ── POSITIONS ─────────────────────────────────────────────────────────────────
@app.post("/positions")
async def positions(req: SessionRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.get(f"{sess['base_url']}/quick/user/positions",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY})
            data, err = safe_json(r)
            if err:
                return {"success": False, "error": err}
            return data
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ── ORDERS ────────────────────────────────────────────────────────────────────
@app.post("/orders/list")
async def orders_list(req: SessionRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.get(f"{sess['base_url']}/quick/user/orders",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY})
            data, err = safe_json(r)
            if err:
                return {"success": False, "error": err}
            return data
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/place")
async def place_order(req: OrderRequest):
    # Extra check: orders have stricter 200/minute limit
    if not rl.check_order():
        raise HTTPException(
            status_code=429,
            detail=f"Order rate limit exceeded: 200 orders/minute max (Kotak NEO API policy). Status: {rl.status()['order_api']}"
        )
    sess = get_session(req.session_id)
    jData = {"am": req.amo, "dq": req.disclosed_quantity, "es": req.exchange_segment,
             "mp": req.market_protection, "pc": req.product, "pf": req.pf,
             "pr": req.price, "pt": req.order_type, "qt": req.quantity,
             "rt": req.validity, "tp": req.trigger_price, "ts": req.trading_symbol, "tt": req.transaction_type}
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(f"{sess['base_url']}/quick/order/rule/ms/place",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"],
                         "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/x-www-form-urlencoded"},
                data={"jData": json.dumps(jData)})
            result, err = safe_json(r)
            if err:
                return {"success": False, "error": err}
            logger.info(f"Order placed: {result}")
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/cancel")
async def cancel_order(req: CancelRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(f"{sess['base_url']}/quick/order/cancel",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"],
                         "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/x-www-form-urlencoded"},
                data={"jData": json.dumps({"am": req.am, "on": req.order_no})})
            data, err = safe_json(r)
            if err:
                return {"success": False, "error": err}
            return data
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ── TRADES ────────────────────────────────────────────────────────────────────
@app.post("/trades")
async def trades(req: SessionRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.get(f"{sess['base_url']}/quick/user/trades",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY})
            data, err = safe_json(r)
            if err:
                return {"success": False, "error": err}
            return data
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
