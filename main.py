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
import httpx, json, asyncio, time, re
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

# Scrip master + risk engine. Single instances per process — both are
# in-memory caches that don't need per-session isolation.
from scrip import ScripMaster
from risk import RiskEngine
from ticks import TickRecorder
scrip_master = ScripMaster()
risk_engine = RiskEngine()
tick_recorder = TickRecorder()


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
                blob = {
                    "access_token":  req.access_token,   # required for /script-details/* REST quotes
                    "session_token": d["data"]["token"],
                    "session_sid":   d["data"]["sid"],
                    "server_id":     d["data"].get("sid", ""),
                    "base_url":      d["data"].get("baseUrl", "https://cis.kotaksecurities.com"),
                    "hsServerId":    d["data"].get("hsServerId", ""),
                    "created_at":    datetime.now().isoformat()
                }
                sessions[sid] = blob
                _sessions_save()
                # Return the full blob so the browser can persist it to
                # localStorage. This lets the UI recover across:
                #   (a) a tab refresh (no backend round-trip required), and
                #   (b) a Railway redeploy that wipes sessions.json — the
                #       client posts the blob back to /auth/rehydrate to
                #       reseed the in-memory entry.
                # SECURITY NOTE: storing tokens in localStorage exposes them
                # to XSS. For a single-user personal bot served from your
                # own Railway app this is an acceptable trade-off; for any
                # multi-user deployment, switch to httpOnly cookies.
                return {
                    "success":      True,
                    "session_id":   sid,
                    "session_blob": blob,
                    "ttl_hours":    SESSION_TTL_HOURS,
                }
            return {"success": False, "message": d.get("message", "MPIN failed")}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


# ── SESSION PERSISTENCE: check + rehydrate ───────────────────────────────────
class CheckRequest(BaseModel):
    session_id: str


@app.post("/auth/check")
async def auth_check(req: CheckRequest):
    """Fast 'is this session still in memory?' probe. The frontend calls this
    on page load before any other API. Does NOT touch Kotak — the live-API
    health is implicitly tested by the next real call (which will return 401
    if the Kotak-side session has expired)."""
    sess = sessions.get(req.session_id)
    if not sess:
        return {"alive": False, "reason": "not_in_memory"}
    try:
        ts = datetime.fromisoformat(sess.get("created_at", "")).timestamp()
    except ValueError:
        ts = 0
    age_h = (datetime.now().timestamp() - ts) / 3600 if ts else 999
    return {
        "alive":       age_h < SESSION_TTL_HOURS,
        "age_hours":   round(age_h, 2),
        "ttl_hours":   SESSION_TTL_HOURS,
    }


class RehydrateRequest(BaseModel):
    session_id:    str
    session_blob:  dict


@app.post("/auth/rehydrate")
async def auth_rehydrate(req: RehydrateRequest):
    """Re-seed the in-memory session entry from a client-supplied blob.

    The frontend calls this if /auth/check says 'not_in_memory' (i.e. the
    backend was redeployed and lost its in-memory + on-disk sessions). The
    client's localStorage holds the full blob from the last /auth/validate,
    so we can restore everything without forcing the user back to login.
    """
    blob = req.session_blob or {}
    required = {"access_token", "session_token", "session_sid", "base_url"}
    missing = required - set(blob.keys())
    if missing:
        return {"alive": False, "reason": f"blob missing fields: {sorted(missing)}"}
    try:
        ts = datetime.fromisoformat(blob.get("created_at", "")).timestamp()
    except ValueError:
        return {"alive": False, "reason": "invalid created_at"}
    age_h = (datetime.now().timestamp() - ts) / 3600
    if age_h >= SESSION_TTL_HOURS:
        return {"alive": False, "reason": f"expired (age {age_h:.1f}h >= ttl {SESSION_TTL_HOURS}h)"}
    sessions[req.session_id] = blob
    _sessions_save()
    logger.info(f"session rehydrated: {req.session_id} (age {age_h:.1f}h)")
    return {"alive": True, "session_id": req.session_id, "age_hours": round(age_h, 2)}


# ── WALLET / LIMITS ───────────────────────────────────────────────────────────
# Kotak's /quick/user/limits ships the same numbers under wildly different
# key names across account variants. The frontend only knows the short
# camelCase names (avlCash, avlMrgn, …) — we map every known Kotak alias to
# those canonical keys here so the UI Just Works regardless of which shape
# Kotak's gateway returns for a given account.
WALLET_ALIASES = {
    "avlCash":   ["avlCash", "AvailableCash", "availableCash", "Net", "net",
                  "Cash", "cash", "CashAvailable", "cashAvailable",
                  "AvlCash", "AvlCashBal", "openingCashBalance"],
    "avlMrgn":   ["avlMrgn", "AvailableMargin", "availableMargin",
                  "marginAvailable", "AvlMrgn"],
    "mrgnUsd":   ["mrgnUsd", "MarginUsed", "marginUsed", "usedMargin",
                  "MrgnUsd", "marginUtilised"],
    "insufFund": ["insufFund", "InsufficientFund", "insufficientFund",
                  "InsufFund"],
    "rmsVldtd":  ["rmsVldtd", "RmsValidate", "rmsValidate", "rms", "RmsVldtd"],
}


def _clean_num(v):
    """Kotak sometimes returns '1,800.00' or ' 1800 '; parseFloat in JS would
    stop at the comma. Strip whitespace + commas, leave non-numeric strings
    (like 'OK' for rmsVldtd) untouched."""
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v.strip().replace(",", "")
        try:
            float(s)
            return s
        except ValueError:
            return v
    return v


@app.post("/wallet/limits")
async def limits(req: SessionRequest):
    """Return Kotak limits flattened + alias-normalized so the UI can read
    avlCash/avlMrgn/etc. at the top level regardless of which Kotak field
    naming convention this account uses."""
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

            # Step 1: flatten any data/Data wrapper.
            flat: dict = {}
            if isinstance(raw, dict):
                inner = raw.get("data") or raw.get("Data")
                if isinstance(inner, list) and inner:
                    inner = inner[0]
                if isinstance(inner, dict):
                    flat.update(inner)
                for k, v in raw.items():
                    if k not in ("data", "Data") and not isinstance(v, (dict, list)):
                        flat.setdefault(k, v)
            elif isinstance(raw, list) and raw and isinstance(raw[0], dict):
                flat.update(raw[0])

            # Step 2: build alias index (case-insensitive) into the flat dict.
            ci = {k.lower(): k for k in flat.keys()}
            normalized: dict = {}
            sources: dict = {}  # canonical -> the actual Kotak field name we picked
            for canonical, aliases in WALLET_ALIASES.items():
                for alias in aliases:
                    real = ci.get(alias.lower())
                    if real is not None and flat.get(real) not in (None, ""):
                        normalized[canonical] = _clean_num(flat[real])
                        sources[canonical] = real
                        break

            logger.info(
                f"Wallet limits: normalized={normalized} sources={sources} "
                f"flat_keys={list(flat.keys())[:20]}"
            )
            # Push the available margin into the risk engine. avlMrgn is
            # "Available Margin for Buying Options" — the field the user
            # wants used as the live wallet (delayed settlements still show
            # here even when avlCash hasn't updated yet).
            wallet_value = normalized.get("avlMrgn") or normalized.get("avlCash") or 0
            try:
                risk_engine.set_wallet(float(wallet_value))
            except (TypeError, ValueError):
                pass
            return {
                "success": True,
                **normalized,
                "_sources": sources,           # which Kotak field gave each canonical value
                "_keys": sorted(flat.keys()),  # every flat key Kotak returned
                "_raw": raw,
                "risk":  risk_engine.state(),
            }
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
async def _get_quote_item(c: httpx.AsyncClient, sess: dict, neo_symbol: str, exch: str):
    """Fetch the full Kotak quote item dict via /script-details/1.0/quotes/
    neosymbol/{exch}|{symbol}/ltp. Returns (item_dict_or_None, error_str).

    The endpoint returns more than just LTP — change %, prev close, OHLC,
    volume, OI are all in the same response item under various aliases
    (Kotak's field names differ by segment and account variant). Callers
    that need more than the price should call this directly and use a
    case-insensitive alias picker on the returned dict.
    """
    base = sess["base_url"].rstrip("/")
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
    return item, None


async def _ltp_via_script_details(c: httpx.AsyncClient, sess: dict, neo_symbol: str, exch: str):
    """Thin wrapper over _get_quote_item — returns (ltp_float|None, error)."""
    item, err = await _get_quote_item(c, sess, neo_symbol, exch)
    if err or not item:
        return None, err
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
        # Kotak's /script-details/.../ltp response is verified to contain
        # ONLY the LTP — no change, no prev_close, no OHLC. (We confirmed
        # this by dumping the raw item dict on the user's account: it
        # carries just exchange_token, display_symbol, exchange, ltp.)
        # So we get the live price from Kotak and the day-change reference
        # from Yahoo Finance's public chart endpoint, which returns
        # prev_close + OHLC for ^NSEI / ^BSESN with no auth.
        ltp_item, ltp_err = await _get_quote_item(c, sess, inst["neo_symbol"], inst["exchange_segment"])
        ltp_vix,  err_vix  = await _ltp_via_script_details(c, sess, vix["neo_symbol"],  vix["exchange_segment"])
        yahoo_data, yahoo_err = await _ohlc_via_yahoo(c, req.instrument)
        # Kotak fallback OHLC paths — kept for diagnostics; rarely populated.
        ohlc_raw, ohlc_err = await _ohlc_via_script_details(c, sess, inst["neo_symbol"], inst["exchange_segment"])
        qq_raw, qq_err = await _ohlc_via_quick_quotes(c, sess, inst)

    ltp_inst = None
    if isinstance(ltp_item, dict):
        try:
            ltp_inst = float(ltp_item.get("ltp") or 0)
        except (TypeError, ValueError):
            ltp_inst = None
    err_inst = ltp_err if not ltp_inst else None

    def _pick(item, *keys):
        """Kotak ships open/high/low/prev-close under several aliases
        depending on the segment (op/open/openPrice; c/close/prevClose).
        Lower-case lookup returns the first non-empty match as float."""
        if not isinstance(item, dict):
            return None
        ci = {k.lower(): k for k in item.keys()}
        for k in keys:
            rk = ci.get(k.lower())
            if rk and item.get(rk) not in (None, "", "0", 0):
                try:
                    return float(item[rk])
                except (TypeError, ValueError):
                    continue
        return None

    # Pick fields from sources in priority order:
    #   1. yahoo_data — public Yahoo Finance v8, populates open/high/low/
    #                   prev_close for ^NSEI / ^BSESN reliably
    #   2. ltp_item   — Kotak; carries ltp on every account variant, the
    #                   richer fields are usually empty here but harmless
    #                   to check
    #   3. qq_raw     — Kotak /quick/quotes (rarely populated on the
    #                   accounts we've inspected)
    #   4. ohlc_raw   — Kotak /script-details/.../ohlc (often 404)
    def _from_any(*keys):
        for src in (yahoo_data, ltp_item, qq_raw, ohlc_raw):
            v = _pick(src, *keys)
            if v is not None:
                return v
        return None
    real_open  = _from_any("o", "op", "open", "openPrice", "open_price")
    real_high  = _from_any("h", "hp", "high", "highPrice", "dayHigh", "high_price")
    real_low   = _from_any("l", "lp", "low",  "lowPrice",  "dayLow",  "low_price")
    prev_close = _from_any("c",  "close", "prevClose", "previousClose", "prev_close",
                           "previous_close", "closePrice", "yc", "ycp", "cls", "prevclose")
    # Many Kotak account variants ship the change ready-computed under
    # one of these keys. If we find it, use it directly — no need to
    # compute (ltp - prev_close)/prev_close.
    direct_change_pct = _from_any("ncp", "nc", "pc", "pchg", "perChange",
                                  "percentChange", "pricePctChange",
                                  "chgper", "chgPct", "chgPercent")
    direct_change_abs = _from_any("chg", "ch", "cng", "change", "absoluteChange",
                                  "priceChange", "ltpChange", "chgVal")
    # Diagnostic log — line gets printed to Railway logs on every call.
    # If the next user screenshot still shows +0.00%, this tells me the
    # *exact* keys Kotak returned so I can add the missing alias in one
    # targeted commit.
    logger.info(
        f"market_data ohlc: inst={req.instrument} ltp={ltp_inst} "
        f"open={real_open} high={real_high} low={real_low} prev_close={prev_close} "
        f"yahoo_ok={bool(yahoo_data)} yahoo_err={yahoo_err}"
    )

    if ltp_inst is not None and ltp_inst > 0:
        # Record the tick so momentum_5m still works. We deliberately do
        # NOT use the recorder's "open" / "change" any more — those are
        # session-local to the bot process. Kotak's open is authoritative.
        stats = tick_recorder.record(req.instrument.upper(), ltp_inst)
        if ltp_vix and ltp_vix > 0:
            tick_recorder.record("VIX", ltp_vix)

        # Day-change priority:
        #   1. Kotak's own direct change% if shipped in the response
        #      (matches Neo app to the rounding)
        #   2. Computed from prev_close                     (matches too)
        #   3. Computed from session open                   (close enough)
        #   4. Recorder's first-tick                        (last resort)
        if direct_change_pct is not None:
            change_pct = direct_change_pct
        else:
            ref = prev_close or real_open or stats.get("open") or ltp_inst
            change_pct = ((ltp_inst - ref) / ref * 100) if ref else 0.0
        # Absolute change: prefer Kotak's direct value, else compute.
        if direct_change_abs is not None:
            change_abs = direct_change_abs
        else:
            change_abs = (ltp_inst - prev_close) if prev_close else 0
        return {
            "success":     True,
            "instrument":  req.instrument,
            "ltp":         ltp_inst,
            "vix":         ltp_vix or 0,
            "open":        real_open  or stats.get("open",  ltp_inst),
            "high":        real_high  or stats.get("high",  ltp_inst),
            "low":         real_low   or stats.get("low",   ltp_inst),
            "close":       prev_close or stats.get("open",  ltp_inst),
            "prev_close":  prev_close or 0,
            "change":      round(change_pct, 3),
            "change_abs":  round(change_abs, 2),
            "momentum_5m": stats.get("momentum_5m", 0),
            "ticks_count": stats.get("ticks_count", 1),
            "source":      "script-details/ltp+ohlc" if (ltp_item or ohlc_raw) else "fallback",
            "ohlc_error":  ohlc_err,
            # Raw responses from every source for TEST QUOTE visibility.
            "raw_ltp_item": ltp_item if isinstance(ltp_item, dict) else None,
            "raw_yahoo":    yahoo_data,
            "yahoo_error":  yahoo_err,
            "raw_qq":       qq_raw if isinstance(qq_raw, dict) else None,
            "qq_error":     qq_err,
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


# ── PER-OPTION LTP (used by the position monitor for real TP/SL) ────────────
class OptionLtpRequest(BaseModel):
    session_id: str
    p_symbol:   str      # neosymbol form, e.g. "NIFTY25MAY24500CE"
    exchange:   str = "nse_fo"


# OHLC for an index/equity via Kotak's /quick/quotes POST. This is the
# working path on every account variant we've tested — same one
# /quotes/ohlc uses. Returns the first item dict from response.data
# (Kotak ships {stat, data:[{...}]}) and parses to dict for the alias
# picker in /quotes/market_data. prev_close on this response is the
# previous-day close which is what the Neo app uses as the day-change
# reference.
# Yahoo Finance v8 chart endpoint — public, no auth, returns prev_close +
# OHLC for indices. Used as the authoritative day-change reference
# because Kotak's /script-details/.../ltp endpoint only ships LTP (we
# confirmed by inspecting the raw response on the user's account — the
# entire body is just {exchange_token, display_symbol, exchange, ltp}).
_YAHOO_SYM = {
    "NIFTY":  "^NSEI",
    "SENSEX": "^BSESN",
}
_YAHOO_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


async def _ohlc_via_yahoo(c: httpx.AsyncClient, instrument: str):
    """Return {ltp, open, high, low, prev_close} for the index from Yahoo
    Finance v8, or (None, error). Reads from `meta` block which Yahoo
    populates with the live regular-market snapshot."""
    sym = _YAHOO_SYM.get(instrument.upper())
    if not sym:
        return None, f"no yahoo symbol for {instrument}"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=1d"
    try:
        r = await c.get(url, headers={"User-Agent": _YAHOO_UA, "Accept": "application/json"})
    except httpx.HTTPError as e:
        return None, f"transport: {type(e).__name__}: {e}"
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    data, err = safe_json(r)
    if err:
        return None, err
    try:
        meta = data["chart"]["result"][0]["meta"]
    except (KeyError, IndexError, TypeError) as e:
        return None, f"unexpected shape: {type(e).__name__}"
    return {
        "ltp":        meta.get("regularMarketPrice"),
        "open":       meta.get("regularMarketOpen") or meta.get("chartPreviousClose"),
        "high":       meta.get("regularMarketDayHigh"),
        "low":        meta.get("regularMarketDayLow"),
        "prev_close": meta.get("regularMarketPreviousClose") or meta.get("chartPreviousClose"),
    }, None


async def _ohlc_via_quick_quotes(c: httpx.AsyncClient, sess: dict, inst_token_dict: dict):
    base = sess["base_url"].rstrip("/")
    url  = f"{base}/quick/quotes"
    headers = {
        "Auth":         sess["session_token"],
        "Sid":          sess["session_sid"],
        "neo-fin-key":  NEO_FIN_KEY,
        "Content-Type": "application/json",
    }
    payload = {"instrument_tokens": [inst_token_dict], "quote_type": "ohlc", "isIndex": True}
    try:
        r = await c.post(url, headers=headers, json=payload)
    except httpx.HTTPError as e:
        return None, f"transport: {type(e).__name__}: {e}"
    data, err = safe_json(r)
    if err:
        return None, err
    items = data.get("data") if isinstance(data, dict) else data
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return items[0], None
    if isinstance(data, dict):
        return data, None
    return None, f"unexpected shape: {str(data)[:200]}"


# Full quote helper — same auth + base URL as _ltp_via_script_details, just
# the /ohlc path so we can scrape OI + volume in one round-trip per leg.
async def _ohlc_via_script_details(c: httpx.AsyncClient, sess: dict, neo_symbol: str, exch: str):
    """Hit /script-details/1.0/quotes/neosymbol/{exch}|{symbol}/ohlc.

    Returns (item_dict_or_None, error). Caller pulls oi/vol/ltp keys —
    Kotak's field names vary between accounts so we look for several
    aliases when parsing on the frontend.
    """
    base = sess["base_url"].rstrip("/")
    url  = f"{base}/script-details/1.0/quotes/neosymbol/{exch}|{neo_symbol}/ohlc"
    headers = {"Authorization": sess.get("access_token", ""), "Content-Type": "application/json"}
    try:
        r = await c.get(url, headers=headers)
    except httpx.HTTPError as e:
        return None, f"transport: {type(e).__name__}: {e}"
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    data, err = safe_json(r)
    if err:
        return None, err
    if isinstance(data, list) and data:
        return data[0], None
    if isinstance(data, dict):
        return data.get("data", data) if isinstance(data.get("data"), dict) else data, None
    return None, f"unexpected shape: {str(data)[:120]}"


class OptionFullRequest(BaseModel):
    session_id: str
    legs:       list[dict]    # [{p_symbol, exchange?}, …]


@app.post("/quotes/option_full")
async def option_full(req: OptionFullRequest):
    """Fetch full quote (LTP + OI + Volume + OHLC) for a list of option
    legs in parallel. Used by the Technicals card to show ATM CE/PE
    OI + Volume + PCR. Returns {"legs":[{p_symbol, ok, ltp, oi, vol,
    open, high, low, error}, …]}."""
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=12) as c:
        results = await asyncio.gather(*[
            _ohlc_via_script_details(c, sess, leg.get("p_symbol",""), leg.get("exchange","nse_fo"))
            for leg in req.legs
        ], return_exceptions=True)
    out = []
    for leg, res in zip(req.legs, results):
        entry = {"p_symbol": leg.get("p_symbol",""), "ok": False}
        if isinstance(res, Exception):
            entry["error"] = f"{type(res).__name__}: {res}"
            out.append(entry); continue
        item, err = res
        if err or not isinstance(item, dict):
            entry["error"] = err or "no data"
            out.append(entry); continue
        # Field-alias lookup — Kotak ships these under slightly different
        # key names depending on the segment. Lower-case match for safety.
        ci = {k.lower(): k for k in item.keys()}
        def pick(*keys):
            for k in keys:
                rk = ci.get(k.lower())
                if rk and item.get(rk) not in (None, "", "0"):
                    return item[rk]
            return None
        try:
            entry.update({
                "ok":    True,
                "ltp":   float(pick("ltp","lastTradedPrice","last_price") or 0),
                "oi":    int(float(pick("oi","openInterest","openInt","open_interest") or 0)),
                "vol":   int(float(pick("vol","volume","totalTradedQty","ttv","total_volume") or 0)),
                "open":  float(pick("op","open","openPrice") or 0),
                "high":  float(pick("hp","high","highPrice","dayHigh") or 0),
                "low":   float(pick("lp","low","lowPrice","dayLow") or 0),
            })
        except (TypeError, ValueError) as e:
            entry["error"] = f"parse: {e}"
        out.append(entry)
    return {"legs": out}


@app.post("/quotes/option_ltp")
async def option_ltp(req: OptionLtpRequest):
    """LTP for a single option contract. Used by startMon() to poll real
    market prices instead of simulating drift. Returns {"ltp": float, "ok": bool}.
    """
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=10) as c:
        ltp, err = await _ltp_via_script_details(c, sess, req.p_symbol, req.exchange)
    if ltp is None or ltp <= 0:
        return {"ok": False, "ltp": 0, "error": err or "no data"}
    return {"ok": True, "ltp": ltp}


# ── STRIKE RESOLUTION (frontend calls this right before placing an order) ────
def _fo_segment(instrument: str) -> str:
    """Map underlying → F&O exchange segment. NIFTY/BANKNIFTY/FINNIFTY etc.
    sit on Kotak's nse_fo; SENSEX (and other BSE indices) on bse_fo."""
    return "bse_fo" if instrument.upper() == "SENSEX" else "nse_fo"


class ResolveStrikeRequest(BaseModel):
    session_id: str
    instrument: str               # NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY
    strike:     float
    side:       str               # "CE" or "PE"


@app.post("/chain/resolve_strike")
async def resolve_strike(req: ResolveStrikeRequest):
    """Resolve a strike+side to a real Kotak option contract.

    Returns the trading-symbol (`p_trd_symbol`, used in /orders/place), the
    neosymbol (`p_symbol`, used for LTP polling), the lot size and the live
    LTP. Required because the displayed option chain in bot.html is a local
    Black-Scholes approximation — it has no real Kotak symbols. Live trades
    pull the real contract via this endpoint at order time.
    """
    sess = get_session(req.session_id)
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")

    side = req.side.upper()
    if side not in ("CE", "PE"):
        raise HTTPException(status_code=400, detail="side must be CE or PE")

    # 1. Spot — we feed it into find_atm_chain so it can pick the nearest expiry
    #    and a strike window that contains the requested strike.
    async with httpx.AsyncClient(timeout=10) as c:
        spot, err = await _ltp_via_script_details(c, sess, inst["neo_symbol"], inst["exchange_segment"])
        if not spot or spot <= 0:
            return {"ok": False, "error": f"spot fetch failed: {err}"}

    # 2. Pull the chain wide enough to contain the requested strike.
    step = 100 if req.instrument.upper() == "BANKNIFTY" else 50
    n = max(6, int(abs(req.strike - spot) / step) + 2)
    try:
        chain_meta = await scrip_master.find_atm_chain(sess, req.instrument, spot, n=n)
    except Exception as e:
        return {"ok": False, "error": f"scrip master: {e}"}

    row = next((r for r in chain_meta["strikes"] if r["strike"] == req.strike), None)
    if not row:
        return {"ok": False, "error": f"strike {req.strike} not in chain"}
    leg = row.get("ce") if side == "CE" else row.get("pe")
    if not leg:
        return {"ok": False, "error": f"no {side} leg for strike {req.strike}"}

    # 3. Live LTP for this specific option leg.
    fo_seg = _fo_segment(req.instrument)
    async with httpx.AsyncClient(timeout=10) as c:
        ltp, err = await _ltp_via_script_details(c, sess, leg["p_symbol"], fo_seg)
    if ltp is None or ltp <= 0:
        return {"ok": False, "error": f"option ltp: {err or 'no data'}"}

    return {
        "ok":            True,
        "p_trd_symbol":  leg["p_trd_symbol"],
        "p_symbol":      leg["p_symbol"],
        "lot_size":      leg["lot_size"],
        "ltp":           ltp,
        "expiry":        chain_meta.get("expiry"),
        "spot":          spot,
        "strike":        req.strike,
        "side":          side,
        "exchange_segment": fo_seg,
    }


# ── NEWS SENTIMENT (server-side; browser CORS blocks Indian news sites) ───
_news_cache: dict[str, tuple[float, dict]] = {}
NEWS_TTL = 300  # 5 min — cheap shared cache; clients can call every cycle

_BULL_WORDS = {"surge","rally","gain","gains","upgrade","beat","beats","record",
               "high","strong","rises","jumps","jump","soars","soar","outperform",
               "buy","positive","bullish","accelerate","recover","recovery"}
_BEAR_WORDS = {"plunge","fall","falls","drop","drops","downgrade","miss","misses",
               "weak","cuts","tumbles","tumble","slumps","slump","loss","losses",
               "sell","negative","bearish","decline","sink","sinks","crash"}


class NewsSentimentRequest(BaseModel):
    instrument: str = "NIFTY"


@app.post("/news/sentiment")
async def news_sentiment(req: NewsSentimentRequest):
    """Lightweight news-sentiment input for the AI signal prompt.

    Fetches Google News RSS for the instrument, scores headlines with a
    bull/bear lexicon, returns sentiment_score in [-1, +1] plus the
    headlines. Cached for 5 minutes server-side so per-cycle calls are
    free after the first one.
    """
    key = req.instrument.upper()
    now = time.monotonic()
    hit = _news_cache.get(key)
    if hit and hit[0] > now:
        return {**hit[1], "cached": True}

    query_g = f"{key}+nifty+nse" if key != "NIFTY" else "NIFTY+nse+india"
    query_b = f"{key} nifty nse india"
    sources = [
        ("google", f"https://news.google.com/rss/search?q={query_g}&hl=en-IN&gl=IN&ceid=IN:en"),
        ("bing",   f"https://www.bing.com/news/search?q={query_b.replace(' ', '+')}&format=rss"),
    ]
    headlines: list[str] = []
    errs: list[str] = []
    # Browser-like UA — RSS endpoints often 403 plain-bot user agents.
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
    async with httpx.AsyncClient(timeout=8, headers={"User-Agent": ua,
                                                      "Accept": "application/rss+xml, application/xml, text/xml, */*"}) as c:
        for src_name, url in sources:
            try:
                r = await c.get(url, follow_redirects=True)
                if r.status_code == 200 and r.text:
                    titles = re.findall(r"<title[^>]*>(.*?)</title>", r.text, re.DOTALL)
                    cleaned = [re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", t).strip() for t in titles[1:11]]
                    cleaned = [t for t in cleaned if t and len(t) > 5]
                    if cleaned:
                        headlines.extend(cleaned)
                        break   # first source that returns usable headlines wins
                else:
                    errs.append(f"{src_name}:HTTP {r.status_code}")
            except httpx.HTTPError as e:
                errs.append(f"{src_name}:{type(e).__name__}")
    err = "; ".join(errs) if not headlines and errs else None

    score = 0.0
    matched = 0
    for h in headlines:
        words = set(w.lower() for w in re.findall(r"[A-Za-z]+", h))
        b = len(words & _BULL_WORDS)
        s = len(words & _BEAR_WORDS)
        if b or s:
            matched += 1
            score += (b - s) / max(b + s, 1)
    sentiment = round(score / matched, 3) if matched else 0.0

    payload = {
        "sentiment_score": sentiment,    # -1.0 .. +1.0
        "headlines":       headlines[:8],
        "count":           len(headlines),
        "matched":         matched,
        "error":           err,
        "fetched_at":      int(time.time()),
        "cached":          False,
    }
    _news_cache[key] = (now + NEWS_TTL, payload)
    return payload


# ── TELEGRAM CHANNEL SENTIMENT (public preview scrape) ────────────────────
# Public Telegram channels are readable via https://t.me/s/{username} — the
# HTML page shows the ~20 most recent messages. We scrape that, strip HTML,
# and score with the same bull/bear lexicon as /news/sentiment. Private
# channels are not accessible this way; for those the user would need to
# create a Telegram bot and add it to the channel (see /news/telegram_bot
# — TODO once requested).
_tg_cache: dict[str, tuple[float, dict]] = {}
TG_TTL = 180  # 3 min cache; stock-news channels post frequently


class TelegramNewsRequest(BaseModel):
    channel: str   # username without @, e.g. "stocknews_india"


@app.post("/news/telegram")
async def news_telegram(req: TelegramNewsRequest):
    channel = (req.channel or "").lstrip("@").strip()
    if not channel:
        return {"ok": False, "error": "channel name required"}

    now = time.monotonic()
    hit = _tg_cache.get(channel)
    if hit and hit[0] > now:
        return {**hit[1], "cached": True}

    url = f"https://t.me/s/{channel}"
    headers = {"User-Agent": _YAHOO_UA, "Accept": "text/html,*/*"}
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True, headers=headers) as c:
            r = await c.get(url)
    except httpx.HTTPError as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "channel": channel}
    if r.status_code != 200:
        return {"ok": False, "error": f"HTTP {r.status_code} (channel may be private or not exist)", "channel": channel}

    # Pull <div class="tgme_widget_message_text ..."> contents. Telegram's
    # preview HTML wraps every message body in this class.
    raw_msgs = re.findall(
        r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
        r.text, re.DOTALL
    )
    if not raw_msgs:
        return {
            "ok":      False,
            "channel": channel,
            "error":   "no messages parsed (channel may be private, empty, or not exist)",
            "html_size": len(r.text),
        }

    headlines: list[str] = []
    for body in raw_msgs[-30:]:
        # Strip HTML tags, collapse whitespace.
        clean = re.sub(r"<br\s*/?>", " ", body, flags=re.I)
        clean = re.sub(r"<[^>]+>", " ", clean)
        clean = re.sub(r"&[a-z]+;", " ", clean)
        clean = re.sub(r"\s+", " ", clean).strip()
        if clean and len(clean) >= 10:
            headlines.append(clean[:280])

    score, matched = 0.0, 0
    for h in headlines:
        words = set(w.lower() for w in re.findall(r"[A-Za-z]+", h))
        b = len(words & _BULL_WORDS)
        s = len(words & _BEAR_WORDS)
        if b or s:
            matched += 1
            score += (b - s) / max(b + s, 1)
    sentiment = round(score / matched, 3) if matched else 0.0

    payload = {
        "ok":              True,
        "channel":         channel,
        "sentiment_score": sentiment,
        "headlines":       headlines[-10:],   # most recent 10
        "count":           len(headlines),
        "matched":         matched,
        "fetched_at":      int(time.time()),
        "cached":          False,
    }
    _tg_cache[channel] = (now + TG_TTL, payload)
    return payload


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

# ── OPTION CHAIN ─────────────────────────────────────────────────────────────
class ChainRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    spot:       float = 0.0   # 0 = auto-fetch from /quotes/market_data
    n_strikes:  int = 5       # ATM ± n_strikes


@app.post("/chain/quotes")
async def chain_quotes_legacy(req: ChainRequest):
    """Backwards-compat alias — frontend used to call this name."""
    req2 = ChainRequest(
        session_id=req.session_id, instrument=req.instrument,
        spot=req.spot or 0, n_strikes=req.n_strikes,
    )
    return await chain_atm(req2)


@app.post("/chain/atm")
async def chain_atm(req: ChainRequest):
    """Real option chain for ATM ± n strikes from Kotak NEO.

    Resolves each strike to its pSymbol via the scrip master, then fetches
    LTP through /script-details/1.0/quotes/neosymbol/{neo_symbol}/ltp — the
    same path that's working for the spot quote.

    Rate-limit aware: we cap at 10 requests per second, batched. The loop
    yields between batches so the global rate limiter doesn't blocking-throttle.
    """
    sess = get_session(req.session_id)
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")

    # 1. Spot — needed to compute ATM. Caller may pass it; otherwise fetch.
    spot = req.spot
    if spot <= 0:
        async with httpx.AsyncClient(timeout=10) as c:
            ltp, err = await _ltp_via_script_details(c, sess, inst["neo_symbol"], inst["exchange_segment"])
            if ltp is None or ltp <= 0:
                return {"success": False, "error": f"spot fetch failed: {err}", "instrument": req.instrument}
            spot = ltp

    # 2. Scrip master — find ATM±n strikes for nearest expiry of this underlying.
    try:
        chain_meta = await scrip_master.find_atm_chain(sess, req.instrument, spot, n=req.n_strikes)
    except Exception as e:
        logger.error(f"chain/atm scrip master: {e}")
        return {"success": False, "error": f"scrip master: {e}", "instrument": req.instrument, "spot": spot}

    if not chain_meta["strikes"]:
        return {
            "success": False,
            "error": "no option strikes resolved (scrip master may still be loading)",
            "instrument": req.instrument, "spot": spot,
            "scrip_status": "ok",
        }

    # 3. Fetch LTPs for each leg via /script-details. Batch in groups of 8 to
    #    stay under the 10 req/sec cap (we leave headroom for other endpoints).
    legs: list[tuple[str, str, dict]] = []  # (strike_key, side, leg_info)
    for row in chain_meta["strikes"]:
        for side in ("ce", "pe"):
            leg = row.get(side)
            if leg and leg.get("p_symbol"):
                legs.append((str(row["strike"]), side, leg))

    out_strikes: dict[float, dict] = {
        r["strike"]: {"strike": r["strike"], "atm": r["strike"] == chain_meta["atm"], "ce": None, "pe": None}
        for r in chain_meta["strikes"]
    }

    fo_seg = _fo_segment(req.instrument)
    async with httpx.AsyncClient(timeout=15) as c:
        for i in range(0, len(legs), 8):
            batch = legs[i:i + 8]
            results = await asyncio.gather(*[
                _ltp_via_script_details(c, sess, leg["p_symbol"], fo_seg)
                for _, _, leg in batch
            ], return_exceptions=True)
            for (strike_key, side, leg), result in zip(batch, results):
                if isinstance(result, Exception):
                    ltp, err = None, str(result)
                else:
                    ltp, err = result
                out_strikes[float(strike_key)][side] = {
                    "ltp":          ltp if ltp is not None else 0.0,
                    "p_symbol":     leg["p_symbol"],
                    "p_trd_symbol": leg["p_trd_symbol"],
                    "lot_size":     leg["lot_size"],
                    "error":        err,
                }
            if i + 8 < len(legs):
                await asyncio.sleep(1.0)  # honour 10 req/sec ceiling

    return {
        "success":    True,
        "instrument": req.instrument,
        "spot":       spot,
        "atm":        chain_meta["atm"],
        "expiry":     chain_meta["expiry"],
        "step":       chain_meta["step"],
        "strikes":    [out_strikes[r["strike"]] for r in chain_meta["strikes"]],
    }


# ── RISK / WALLET-AWARE TRADE GATING ─────────────────────────────────────────
class RiskBookRequest(BaseModel):
    pnl: float


class RiskCheckRequest(BaseModel):
    required_margin: float = 0.0


@app.get("/risk/state")
async def risk_state():
    return risk_engine.state()


@app.post("/risk/check")
async def risk_check(req: RiskCheckRequest):
    ok, reason = risk_engine.can_trade(req.required_margin)
    return {"allowed": ok, "reason": reason, "state": risk_engine.state()}


@app.post("/risk/book")
async def risk_book(req: RiskBookRequest):
    risk_engine.book_trade(req.pnl)
    return {"success": True, "state": risk_engine.state()}


@app.post("/risk/reset_day")
async def risk_reset_day():
    risk_engine.reset_day()
    return {"success": True, "state": risk_engine.state()}


@app.get("/scrip/status")
async def scrip_status():
    """Surface what's cached in the scrip master loader for debugging."""
    cached = sorted(scrip_master._cache.keys())
    return {
        "cached_segments_today": [seg for seg, _ in cached],
        "cache_keys":            [{"segment": s, "date": d, "rows": len(scrip_master._cache[(s, d)])} for s, d in cached],
        "paths_cached":          scrip_master._paths_cache[0] if scrip_master._paths_cache else None,
    }

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

    # ── Step-by-step debug trace ────────────────────────────────────────────
    # When orders fail at Kotak (HTTP 401 stCode 100008 is the case in play)
    # the frontend was previously left with a generic "?". This trace makes
    # every phase visible: payload sent, HTTP status received, body snippet,
    # parse outcome, final fields. Always returned on non-Ok responses;
    # always written to the server log at INFO/WARNING.
    debug = []
    url = f"{sess['base_url']}/quick/order/rule/ms/place"
    debug.append({
        "step": "pre",
        "url":  url,
        "ts":   req.trading_symbol,
        "tt":   req.transaction_type,
        "qt":   req.quantity,
        "pt":   req.order_type,
        "pc":   req.product,
        "es":   req.exchange_segment,
        "header_keys": ["Auth", "Sid", "neo-fin-key", "Content-Type"],
        "jdata_keys":  sorted(jData.keys()),
    })
    logger.info(f"order/place pre: ts={req.trading_symbol} tt={req.transaction_type} qt={req.quantity} pt={req.order_type}")

    # Raw-string body. Critical for Kotak: passing `data={"jData": ...}` makes
    # httpx URL-encode the JSON ({ → %7B, " → %22 …) and Kotak's order
    # endpoint does not URL-decode the form value, so the request is silently
    # rejected as malformed (manifests as HTTP 401 stCode 100008
    # 'unauthorized'). The user's working Python reference (requests.post,
    # data=f"jData={json.dumps(...)}") sends the JSON literal — we match that
    # exact byte-pattern here via httpx.content=.
    body = f"jData={json.dumps(jData)}"
    debug[-1]["body_size"] = len(body)
    async with httpx.AsyncClient(timeout=15) as c:
        t0 = time.monotonic()
        try:
            r = await c.post(url,
                headers={"accept": "application/json",
                         "Auth": sess["session_token"], "Sid": sess["session_sid"],
                         "neo-fin-key": NEO_FIN_KEY,
                         "Content-Type": "application/x-www-form-urlencoded"},
                content=body)
        except httpx.HTTPError as e:
            debug.append({"step": "transport_error", "kind": type(e).__name__, "msg": str(e)})
            logger.warning(f"order/place transport_error: {type(e).__name__}: {e}")
            return {"success": False, "stat": "Not_Ok", "error": f"{type(e).__name__}: {e}", "debug": debug}
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        debug.append({
            "step":         "http_response",
            "status_code":  r.status_code,
            "content_type": r.headers.get("content-type", ""),
            "body_snippet": (r.text or "")[:500],
            "elapsed_ms":   elapsed_ms,
        })

        result, err = safe_json(r)
        debug.append({
            "step":          "parsed",
            "parser_error":  err,
            "parsed_keys":   sorted(result.keys()) if isinstance(result, dict) else None,
            "stat":          result.get("stat") if isinstance(result, dict) else None,
            "nOrdNo":        result.get("nOrdNo") if isinstance(result, dict) else None,
            "stCode":        result.get("stCode") if isinstance(result, dict) else None,
            "emsg":          result.get("emsg") if isinstance(result, dict) else None,
        })

        if err:
            logger.warning(f"order/place parse_error http={r.status_code} err={err}")
            return {"success": False, "stat": "Not_Ok", "error": err, "debug": debug}

        if isinstance(result, dict) and result.get("stat") == "Ok":
            logger.info(f"Order placed: {result}")
            return {**result, "debug": debug}

        logger.warning(
            f"order/place NOT OK http={r.status_code} stCode={result.get('stCode') if isinstance(result, dict) else '?'} "
            f"emsg={result.get('emsg') if isinstance(result, dict) else '?'}"
        )
        # Always include the debug trace when the order didn't come back Ok.
        if isinstance(result, dict):
            return {**result, "debug": debug}
        return {"success": False, "stat": "Not_Ok", "raw": result, "debug": debug}

@app.post("/orders/cancel")
async def cancel_order(req: CancelRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(f"{sess['base_url']}/quick/order/cancel",
                headers={"accept": "application/json",
                         "Auth": sess["session_token"], "Sid": sess["session_sid"],
                         "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/x-www-form-urlencoded"},
                content=f"jData={json.dumps({'am': req.am, 'on': req.order_no})}")
            data, err = safe_json(r)
            if err:
                return {"success": False, "error": err}
            return data
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


# ── ORDER PLACEMENT SMOKE TEST ───────────────────────────────────────────────
class TestPlaceRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    price:      float = 0.05    # buy LIMIT at 5p — practically can't fill
    qty_lots:   int   = 1


@app.post("/orders/test_place")
async def test_place(req: TestPlaceRequest):
    """Place a tiny far-OTM BUY LIMIT @ ₹0.05 and immediately cancel it.

    Goal: surface the *real* Kotak response so we can see whether the
    server's outbound IP is whitelisted for trading. We auto-pick an ATM
    CE for the nearest expiry from the scrip master, post the order, then
    cancel it whether it acked or errored.

    Why this is safe:
      - LIMIT (not MKT), so no chance of slippage to a fill price.
      - BUY at ₹0.05 — 5 paise is the minimum tick; nobody sells options
        at that price, so the order sits in the book until cancel.
      - Max possible loss if it somehow filled = ₹0.05 × lot_size × qty_lots.
        For NIFTY 1 lot that's ₹0.05 × 75 = ₹3.75.
      - We cancel it within ~1 second of acceptance.
    """
    sess = get_session(req.session_id)

    # 1. Resolve a tradable ATM CE symbol.
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")

    spot = 0.0
    async with httpx.AsyncClient(timeout=15) as c:
        ltp, err = await _ltp_via_script_details(c, sess, inst["neo_symbol"], inst["exchange_segment"])
        if not ltp or ltp <= 0:
            return {"success": False, "stage": "spot_fetch", "error": err or "spot is 0"}
        spot = ltp

    try:
        chain_meta = await scrip_master.find_atm_chain(sess, req.instrument, spot, n=2)
    except Exception as e:
        return {"success": False, "stage": "scrip_master", "error": str(e), "spot": spot}

    atm_row = next((s for s in chain_meta["strikes"] if s["strike"] == chain_meta["atm"] and s.get("ce")), None)
    if not atm_row:
        return {"success": False, "stage": "atm_lookup", "error": "no ATM CE in scrip master", "spot": spot}

    ce = atm_row["ce"]
    pTrdSymbol = ce["p_trd_symbol"]
    pSymbol    = ce["p_symbol"]
    fo_seg     = _fo_segment(req.instrument)
    default_lot = 20 if req.instrument.upper() == "SENSEX" else 75
    lot_size   = ce["lot_size"] or default_lot
    qty        = req.qty_lots * lot_size

    # 2. Fetch the option's real LTP. We use this to set a sane limit price
    #    far below market (max(0.05, ltp*0.6)) — far enough that the order
    #    will not fill, near enough that Kotak's circuit-range check accepts
    #    it. A flat ₹0.05 on a ₹100 option is outside the typical ±20%
    #    circuit and is itself rejected as out-of-range, which can manifest
    #    as the same opaque 'unauthorized' response we are trying to test.
    async with httpx.AsyncClient(timeout=10) as c:
        opt_ltp, _ = await _ltp_via_script_details(c, sess, pSymbol, fo_seg)
    safe_limit = req.price
    if opt_ltp and opt_ltp > 0:
        safe_limit = max(0.05, round(opt_ltp * 0.6, 2))

    # 3. Place the BUY LIMIT order. Capture *everything* — status, headers,
    #    body — so any IP-whitelist or permissions error is visible.
    place_url = f"{sess['base_url']}/quick/order/rule/ms/place"
    jData = {
        "am": "NO", "dq": "0", "es": fo_seg, "mp": "0",
        "pc": "MIS", "pf": "N",
        "pr": f"{safe_limit:.2f}",
        "pt": "L",                  # LIMIT
        "qt": str(qty),
        "rt": "DAY", "tp": "0",
        "ts": pTrdSymbol,
        "tt": "B",
    }
    place_diag: dict = {
        "url":        place_url,
        "trading_symbol": pTrdSymbol,
        "p_symbol":   pSymbol,
        "qty":        qty,
        "option_ltp": opt_ltp,
        "limit_price": safe_limit,
        "atm_strike": chain_meta["atm"],
        "spot":       spot,
        "expiry":     chain_meta.get("expiry"),
    }
    # Raw-string body — must match user's working Python reference exactly.
    # See /orders/place for the full explanation; URL-encoded form data is
    # what Kotak silently rejects with 401 stCode 100008.
    body = f"jData={json.dumps(jData)}"
    place_diag["body_size"] = len(body)
    async with httpx.AsyncClient(timeout=20) as c:
        try:
            r = await c.post(place_url,
                headers={"accept": "application/json",
                         "Auth": sess["session_token"], "Sid": sess["session_sid"],
                         "neo-fin-key": NEO_FIN_KEY,
                         "Content-Type": "application/x-www-form-urlencoded"},
                content=body)
        except httpx.HTTPError as e:
            place_diag["transport_error"] = f"{type(e).__name__}: {e}"
            return {"success": False, "stage": "place_http", **place_diag}

        place_diag["http_status"]    = r.status_code
        place_diag["content_type"]   = r.headers.get("content-type", "")
        place_diag["body_snippet"]   = (r.text or "")[:600]
        body, parse_err = safe_json(r)
        place_diag["parse_error"]    = parse_err
        place_diag["body"]           = body

    # 3. If we got an order number, cancel immediately.
    order_no = None
    if isinstance(body, dict):
        order_no = body.get("nOrdNo") or body.get("orderNo") or body.get("ordNo")
    place_diag["order_no"] = order_no

    cancel_diag: dict = {}
    if order_no:
        async with httpx.AsyncClient(timeout=15) as c:
            try:
                cr = await c.post(f"{sess['base_url']}/quick/order/cancel",
                    headers={"accept": "application/json",
                             "Auth": sess["session_token"], "Sid": sess["session_sid"],
                             "neo-fin-key": NEO_FIN_KEY,
                             "Content-Type": "application/x-www-form-urlencoded"},
                    content=f"jData={json.dumps({'am': 'NO', 'on': str(order_no)})}")
                cb, cerr = safe_json(cr)
                cancel_diag = {
                    "http_status": cr.status_code,
                    "body":        cb,
                    "parse_error": cerr,
                }
            except httpx.HTTPError as e:
                cancel_diag = {"transport_error": f"{type(e).__name__}: {e}"}

    # 4. Verdict for the frontend.
    accepted = bool(order_no)
    rejected_reason = None
    if not accepted and isinstance(body, dict):
        rejected_reason = body.get("emsg") or body.get("message") or body.get("errMsg")
    return {
        "success":          accepted,
        "verdict":          "accepted (and cancelled)" if accepted else "rejected",
        "rejected_reason":  rejected_reason,
        "ip_whitelist_hint": (
            "Look for 'IP', 'whitelist', 'access denied', '403', or 'Forbidden' in body_snippet/rejected_reason. "
            "If absent and order was accepted, the IP is fine."
        ),
        "place":            place_diag,
        "cancel":           cancel_diag,
    }

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
