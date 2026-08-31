from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
import os
import httpx, json, asyncio, time, re, sqlite3, hashlib, html
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
from collections import deque
from pydantic import BaseModel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_json(r: httpx.Response):
    text = (r.text or "").strip()
    ctype = r.headers.get("content-type", "")
    if not text:
        return None, f"empty body (HTTP {r.status_code})"
    try:
        return r.json(), None
    except (json.JSONDecodeError, ValueError):
        snippet = text[:300].replace("\n", " ").replace("\r", " ")
        return None, f"non-JSON body (HTTP {r.status_code}, content-type={ctype}): {snippet}"

def _clean_symbol(s: str) -> str:
    if not s:
        return ""
    return s.lower().replace(" ", "").replace("_", "").replace("-", "").strip()

app = FastAPI(title="NEXUS Trading Bot v3")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class RateLimiter:
    def __init__(self):
        self.general_window  = deque()
        self.general_limit   = 9
        self.general_period  = 1.0
        self._lock = asyncio.Lock()
        self._last_request_time = 0.0
        self._min_spacing = 0.12
        self.order_window    = deque()
        self.order_limit     = 200
        self.order_period    = 60.0
        self.total_blocked   = 0
        self.total_allowed   = 0

    def _clean(self, dq: deque, period: float) -> None:
        cutoff = time.monotonic() - period
        while dq and dq[0] < cutoff:
            dq.popleft()

    async def acquire_slot(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._min_spacing:
                delay = self._min_spacing - elapsed
                await asyncio.sleep(delay)
                now = time.monotonic()
            self._clean(self.general_window, self.general_period)
            while len(self.general_window) >= self.general_limit:
                oldest = self.general_window[0]
                wait_time = (oldest + self.general_period) - now
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                now = time.monotonic()
                self._clean(self.general_window, self.general_period)
            self.general_window.append(now)
            self._last_request_time = now
            self.total_allowed += 1

    def check_order(self) -> bool:
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

rl = RateLimiter()

@app.get("/rate_limit/status")
async def rate_limit_status():
    return rl.status()

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    path = request.url.path
    protected = ["/auth/", "/wallet/", "/quotes/", "/orders/", "/positions", "/trades", "/chain/", "/scrip/"]
    if any(path.startswith(p) for p in protected):
        try:
            await rl.acquire_slot()
        except Exception as e:
            return JSONResponse(status_code=429, content={"error": "Rate limiter queue delay failed", "detail": str(e)})
    response = await call_next(request)
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
    _news_db_init()

from streamer import KotakStreamer
streamers: dict[str, KotakStreamer] = {}

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

INSTRUMENT_TOKENS = {
    "NIFTY":       {"instrument_token": "26000", "exchange_segment": "nse_cm", "neo_symbol": "Nifty 50"},
    "BANKNIFTY":   {"instrument_token": "26009", "exchange_segment": "nse_cm", "neo_symbol": "Nifty Bank"},
    "FINNIFTY":    {"instrument_token": "26037", "exchange_segment": "nse_cm", "neo_symbol": "Nifty Fin Service"},
    "MIDCPNIFTY":  {"instrument_token": "26074", "exchange_segment": "nse_cm", "neo_symbol": "NIFTY MID SELECT"},
    "SENSEX":      {"instrument_token": "1",     "exchange_segment": "bse_cm", "neo_symbol": "SENSEX"},
    "VIX":         {"instrument_token": "26017", "exchange_segment": "nse_cm", "neo_symbol": "INDIA VIX"},
}

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

class CheckRequest(BaseModel):
    session_id: str

def get_session(sid):
    if sid not in sessions:
        raise HTTPException(status_code=401, detail="Session expired. Login again.")
    return sessions[sid]

class ClientLogRequest(BaseModel):
    level: str = "info"
    message: str

@app.post("/client_log")
async def client_log(req: ClientLogRequest):
    """Lets the browser mirror its own log entries into the server log (pm2 logs),
    so client-side skip/block reasons (entry window, cooldown, expiry guard, etc.)
    that never trigger an HTTP call to any other endpoint are still visible here."""
    logger.info(f"[UI] {req.level.upper()}: {req.message}")
    return {"ok": True}

BOT_HTML = open("bot.html").read() if os.path.exists("bot.html") else "<h1>bot.html missing</h1>"

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    return HTMLResponse(content=BOT_HTML)

@app.get("/health")
async def health():
    return {"status": "NEXUS Production Execution Bot Running", "version": "4.0.0-prod", "time": datetime.now().isoformat()}

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
                    "access_token":  req.access_token,
                    "session_token": d["data"]["token"],
                    "session_sid":   d["data"]["sid"],
                    "server_id":     d["data"].get("sid", ""),
                    "base_url":      d["data"].get("baseUrl", "https://cis.kotaksecurities.com"),
                    "hsServerId":    d["data"].get("hsServerId", ""),
                    "created_at":    datetime.now().isoformat()
                }
                sessions[sid] = blob
                _sessions_save()
                return {
                    "success":      True,
                    "session_id":   sid,
                    "session_blob": blob,
                    "ttl_hours":    SESSION_TTL_HOURS,
                }
            return {"success": False, "message": d.get("message", "MPIN failed")}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/auth/check")
async def auth_check(req: CheckRequest):
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

            ci = {k.lower(): k for k in flat.keys()}
            normalized: dict = {}
            sources: dict = {}
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
            wallet_value = normalized.get("avlMrgn") or normalized.get("avlCash") or 0
            try:
                risk_engine.set_wallet(float(wallet_value))
            except (TypeError, ValueError):
                pass
            return {
                "success": True,
                **normalized,
                "_sources": sources,
                "_keys": sorted(flat.keys()),
                "_raw": raw,
                "risk":  risk_engine.state(),
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

async def _get_quote_item(c: httpx.AsyncClient, sess: dict, neo_symbol: str, exch: str):
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
    item, err = await _get_quote_item(c, sess, neo_symbol, exch)
    if err or not item:
        return None, err
    try:
        return float(item.get("ltp") or 0), None
    except (TypeError, ValueError) as e:
        return None, f"ltp parse: {e}"

async def _ohlc_via_script_details(c: httpx.AsyncClient, sess: dict, neo_symbol: str, exch: str):
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

async def _ohlc_via_quick_quotes(c: httpx.AsyncClient, sess: dict, inst_token_dict: dict):
    neo_symbol = inst_token_dict.get("neo_symbol")
    exch = inst_token_dict.get("exchange_segment")
    if not neo_symbol or not exch:
        return None, "invalid inst token format"
    return await _ohlc_via_script_details(c, sess, neo_symbol, exch)

async def _fetch_batch_quotes(c: httpx.AsyncClient, sess: dict, queries: list[str], filter_name: str = "all"):
    base = sess["base_url"].rstrip("/")
    query_str = ",".join(queries)
    url = f"{base}/script-details/1.0/quotes/neosymbol/{query_str}/{filter_name}"
    headers = {
        "Authorization": sess.get("access_token", ""),
        "Content-Type": "application/json"
    }
    try:
        r = await c.get(url, headers=headers)
    except httpx.HTTPError as e:
        return None, f"transport: {type(e).__name__}: {e}"
    data, err = safe_json(r)
    if err:
        return None, err
    return data, None

_YAHOO_SYM = {
    "NIFTY":  "^NSEI",
    "SENSEX": "^BSESN",
    "VIX":    "^INDIAVIX",
}
_YAHOO_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

async def _ohlc_via_yahoo(c: httpx.AsyncClient, instrument: str):
    sym = _YAHOO_SYM.get(instrument.upper())
    if not sym:
        return None, f"no yahoo symbol for {instrument}"
    sym_encoded = sym.replace("^", "%5E")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym_encoded}?interval=1d&range=1d"
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

@app.post("/quotes/ltp")
async def get_ltp(req: QuoteRequest):
    sess = get_session(req.session_id)
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            item, err = await _get_quote_item(c, sess, inst["neo_symbol"], inst["exchange_segment"])
            if err:
                return {"success": False, "instrument": req.instrument, "error": err}
            return {"success": True, "instrument": req.instrument, "data": item}
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
            item, err = await _ohlc_via_script_details(c, sess, inst["neo_symbol"], inst["exchange_segment"])
            if err:
                return {"success": False, "instrument": req.instrument, "error": err}
            return {"success": True, "instrument": req.instrument, "data": item}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/quotes/market_data")
async def get_market_data(req: QuoteRequest):
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    vix  = INSTRUMENT_TOKENS["VIX"]
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")
    sess = get_session(req.session_id)
    if sess:
        try:
            asyncio.create_task(get_streamer(req.session_id))
            s_ws = streamers.get(req.session_id)
            if s_ws:
                asyncio.create_task(s_ws.subscribe([inst, vix]))
        except Exception:
            pass
    async with httpx.AsyncClient(timeout=15) as c:
        queries = [
            f"{inst['exchange_segment']}|{inst['neo_symbol']}",
            f"{vix['exchange_segment']}|{vix['neo_symbol']}"
        ]
        batch_data, batch_err = await _fetch_batch_quotes(c, sess, queries, "all")
        results = await asyncio.gather(
            _ohlc_via_yahoo(c, req.instrument),
            _ohlc_via_yahoo(c, "VIX"),
            return_exceptions=True
        )
        yahoo_data, yahoo_err = results[0] if not isinstance(results[0], Exception) else (None, str(results[0]))
        yahoo_vix, yahoo_vix_err = results[1] if not isinstance(results[1], Exception) else (None, str(results[1]))
    ltp_item = None
    vix_item = None
    target_inst_clean = _clean_symbol(inst["neo_symbol"])
    target_vix_clean  = _clean_symbol(vix["neo_symbol"])
    if isinstance(batch_data, list):
        for item in batch_data:
            if not isinstance(item, dict):
                continue
            exch = item.get("exchange", "") or ""
            token = item.get("exchange_token", "") or ""
            exch_clean = exch.lower().strip()
            token_clean = _clean_symbol(token)
            if exch_clean == inst["exchange_segment"].lower().strip() and token_clean == target_inst_clean:
                ltp_item = item
            elif exch_clean == vix["exchange_segment"].lower().strip() and (token_clean == target_vix_clean or "vix" in token_clean):
                vix_item = item
    ltp_inst = None
    if isinstance(ltp_item, dict):
        try:
            ltp_inst = float(ltp_item.get("ltp") or 0)
        except (TypeError, ValueError):
            ltp_inst = None
    ltp_vix = None
    if isinstance(vix_item, dict):
        try:
            ltp_vix = float(vix_item.get("ltp") or 0)
        except (TypeError, ValueError):
            ltp_vix = None
    if ltp_vix is None or ltp_vix <= 0:
        async with httpx.AsyncClient(timeout=8) as c:
            vix_single, _ = await _get_quote_item(c, sess, "INDIA VIX", vix["exchange_segment"])
            if isinstance(vix_single, dict):
                try:
                    val = float(vix_single.get("ltp") or 0)
                    if val > 0:
                        ltp_vix = val
                        vix_item = vix_single
                except (TypeError, ValueError):
                    pass
    if ltp_vix is None or ltp_vix <= 0:
        try:
            s_ws = streamers.get(req.session_id)
            if s_ws:
                v_tick = s_ws.get_tick(vix["instrument_token"])
                if v_tick and v_tick.get("ltp") > 0:
                    ltp_vix = v_tick["ltp"]
        except Exception:
            pass
    if ltp_vix is None or ltp_vix <= 0:
        if isinstance(yahoo_vix, dict) and yahoo_vix.get("ltp"):
            ltp_vix = yahoo_vix["ltp"]
            logger.info(f"Retrieved VIX LTP as {ltp_vix} from Yahoo Finance backup.")
    if ltp_vix is None or ltp_vix <= 0:
        ltp_vix = 15.00
        logger.info("VIX defaulted to stable baseline 15.00 (Kotak REST, WS, and Yahoo all returned 0/error).")
    err_inst = batch_err if not ltp_inst else None
    def _pick(item, *keys):
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
    kotak_ohlc = ltp_item.get("ohlc") if isinstance(ltp_item, dict) else None
    def _from_any(*keys):
        for src in (yahoo_data, kotak_ohlc, ltp_item):
            v = _pick(src, *keys)
            if v is not None:
                return v
        return None
    real_open  = _from_any("o", "op", "open", "openPrice", "open_price")
    real_high  = _from_any("h", "hp", "high", "highPrice", "dayHigh", "high_price")
    real_low   = _from_any("l", "lp", "low",  "lowPrice",  "dayLow",  "low_price")
    prev_close = _from_any("c",  "close", "prevClose", "previousClose", "prev_close",
                           "previous_close", "closePrice", "yc", "ycp", "cls", "prevclose")
    direct_change_pct = _from_any("ncp", "nc", "pc", "pchg", "per_change", "perChange",
                                  "percentChange", "pricePctChange", "chgper", "chgPct", "chgPercent")
    direct_change_abs = _from_any("chg", "ch", "cng", "change", "absoluteChange",
                                  "priceChange", "ltpChange", "chgVal")
    logger.info(
        f"market_data ohlc (BATCHED): inst={req.instrument} ltp={ltp_inst} "
        f"open={real_open} high={real_high} low={real_low} prev_close={prev_close} "
        f"yahoo_ok={bool(yahoo_data)} yahoo_err={yahoo_err}"
    )
    if ltp_inst is not None and ltp_inst > 0:
        stats = tick_recorder.record(req.instrument.upper(), ltp_inst)
        if ltp_vix and ltp_vix > 0:
            tick_recorder.record("VIX", ltp_vix)
        if direct_change_pct is not None:
            change_pct = direct_change_pct
        else:
            ref = prev_close or real_open or stats.get("open") or ltp_inst
            change_pct = ((ltp_inst - ref) / ref * 100) if ref else 0.0
        if direct_change_abs is not None:
            change_abs = direct_change_abs
        else:
            change_abs = (ltp_inst - prev_close) if prev_close else 0
        return {
            "success":     True,
            "instrument":  req.instrument,
            "ltp":         ltp_inst,
            "vix":         ltp_vix,
            "open":        real_open  or stats.get("open",  ltp_inst),
            "high":        real_high  or stats.get("high",  ltp_inst),
            "low":         real_low   or stats.get("low",   ltp_inst),
            "close":       prev_close or stats.get("open",  ltp_inst),
            "prev_close":  prev_close or 0,
            "change":      round(change_pct, 3),
            "change_abs":  round(change_abs, 2),
            "momentum_5m": stats.get("momentum_5m", 0),
            "ticks_count": stats.get("ticks_count", 1),
            "source":      "batch/quotes" if ltp_item else "fallback",
            "raw_ltp_item": ltp_item,
            "raw_yahoo":    yahoo_data,
            "yahoo_error":  yahoo_err,
        }
    async with httpx.AsyncClient(timeout=10) as c:
        single_item, single_err = await _get_quote_item(c, sess, inst["neo_symbol"], inst["exchange_segment"])
    if isinstance(single_item, dict) and float(single_item.get("ltp") or 0) > 0:
        ltp_inst = float(single_item["ltp"])
        stats = tick_recorder.record(req.instrument.upper(), ltp_inst)
        kotak_ohlc = single_item.get("ohlc")
        real_open  = _pick(kotak_ohlc, "open") or _pick(single_item, "open") or (yahoo_data.get("open") if yahoo_data else None)
        real_high  = _pick(kotak_ohlc, "high") or _pick(single_item, "high") or (yahoo_data.get("high") if yahoo_data else None)
        real_low   = _pick(kotak_ohlc, "low")  or _pick(single_item, "low")  or (yahoo_data.get("low")  if yahoo_data else None)
        prev_close = _pick(kotak_ohlc, "close") or _pick(single_item, "close", "prev_close") or (yahoo_data.get("prev_close") if yahoo_data else None)
        change_pct = ((ltp_inst - prev_close) / prev_close * 100) if prev_close else 0.0
        return {
            "success":     True,
            "instrument":  req.instrument,
            "ltp":         ltp_inst,
            "vix":         ltp_vix,
            "open":        real_open  or stats.get("open",  ltp_inst),
            "high":        real_high  or stats.get("high",  ltp_inst),
            "low":         real_low   or stats.get("low",   ltp_inst),
            "close":       prev_close or stats.get("open",  ltp_inst),
            "prev_close":  prev_close or 0,
            "change":      round(change_pct, 3),
            "change_abs":  round(ltp_inst - prev_close, 2) if prev_close else 0,
            "momentum_5m": stats.get("momentum_5m", 0),
            "ticks_count": stats.get("ticks_count", 1),
            "source":      "single/backup",
            "raw_ltp_item": single_item,
        }
    try:
        s = await get_streamer(req.session_id)
        await s.subscribe([inst, vix])
        t = s.get_tick(inst["instrument_token"])
        v = s.get_tick(vix["instrument_token"])
        if t and t["ltp"] > 0:
            return {
                "success": True, "instrument": req.instrument,
                "ltp": t["ltp"], "vix": v.get("ltp") if (v and v.get("ltp", 0) > 0) else 15.00,
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
        "ltp": 0, "vix": 15.00, "change": 0,
        "open": 0, "high": 0, "low": 0, "close": 0,
        "error": err_inst or "no quote source returned data",
        "ws": ws_status,
    }

_BULL_WORDS = {"surge", "rally", "gain", "gains", "upgrade", "beat", "beats", "record",
               "high", "strong", "rises", "jumps", "jump", "soars", "soar", "outperform",
               "buy", "positive", "bullish", "accelerate", "recover", "recovery"}
_BEAR_WORDS = {"plunge", "fall", "falls", "drop", "drops", "downgrade", "miss", "misses",
               "weak", "cuts", "tumbles", "tumble", "slumps", "slump", "loss", "losses",
               "sell", "negative", "bearish", "decline", "sink", "sinks", "crash"}


_news_cache: dict[str, tuple[float, dict]] = {}
_tg_cache: dict[str, tuple[float, dict]] = {}  # legacy endpoint kept but not used by execution
TG_TTL = 180
NEWS_TTL = 60
NEWS_MAX_AGE_SECONDS = int(os.environ.get("NEWS_MAX_AGE_SECONDS", "1800"))
NEWS_DB_FILE = os.environ.get("NEWS_DB_FILE", "news_memory.db")

# Free near-live sources. Reality check: free RSS may publish late; the bot rejects
# old items instead of pretending they are live. News is confirmation, not a trade trigger.
_FREE_NEWS_SOURCES = [
    ("google_nifty", "https://news.google.com/rss/search?q=NIFTY%20OR%20SENSEX%20OR%20RBI%20OR%20FII%20OR%20DII%20when:15m&hl=en-IN&gl=IN&ceid=IN:en"),
    ("google_market", "https://news.google.com/rss/search?q=Indian%20stock%20market%20NSE%20BSE%20when:15m&hl=en-IN&gl=IN&ceid=IN:en"),
    ("et_markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("business_standard", "https://www.business-standard.com/rss/markets-106.rss"),
]

_RELEVANCE_TERMS = {
    "nifty", "sensex", "nse", "bse", "banknifty", "market", "markets", "stock", "stocks",
    "rbi", "sebi", "fed", "fii", "dii", "rupee", "dollar", "usd", "crude", "oil",
    "inflation", "cpi", "rate", "repo", "yield", "budget", "election", "global",
    "gift", "sgx", "dow", "nasdaq", "s&p", "earnings",
}

class NewsSentimentRequest(BaseModel):
    instrument: str = "NIFTY"
    max_age_seconds: int = NEWS_MAX_AGE_SECONDS
    force_refresh: bool = False


def _news_db_init() -> None:
    try:
        with sqlite3.connect(NEWS_DB_FILE) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS news_items (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    headline TEXT NOT NULL,
                    link TEXT,
                    published_ts INTEGER,
                    fetched_ts INTEGER NOT NULL,
                    instrument TEXT,
                    sentiment_score REAL DEFAULT 0,
                    impact_score INTEGER DEFAULT 0,
                    freshness_score INTEGER DEFAULT 0,
                    relevance_score INTEGER DEFAULT 0,
                    confidence_score INTEGER DEFAULT 0,
                    used_for_trade INTEGER DEFAULT 0,
                    reasons TEXT
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_news_pub ON news_items(published_ts)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_news_inst ON news_items(instrument)")
            con.commit()
    except Exception as e:
        logger.warning(f"news db init failed: {e}")


def _strip_xml_html(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", text, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_rss_datetime(raw: str) -> int | None:
    raw = _strip_xml_html(raw)
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            continue
    return None


def _extract_rss_items(xml: str, source: str) -> list[dict]:
    items: list[dict] = []
    blocks = re.findall(r"<item\b[^>]*>(.*?)</item>", xml, flags=re.I | re.S)
    if not blocks:
        blocks = re.findall(r"<entry\b[^>]*>(.*?)</entry>", xml, flags=re.I | re.S)
    for b in blocks[:40]:
        title_m = re.search(r"<title[^>]*>(.*?)</title>", b, flags=re.I | re.S)
        if not title_m:
            continue
        headline = _strip_xml_html(title_m.group(1))
        if len(headline) < 8:
            continue
        link_m = re.search(r"<link[^>]*>(.*?)</link>", b, flags=re.I | re.S)
        link = _strip_xml_html(link_m.group(1)) if link_m else ""
        pub = None
        for tag in ("pubDate", "published", "updated", "dc:date"):
            m = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", b, flags=re.I | re.S)
            if m:
                pub = _parse_rss_datetime(m.group(1))
                if pub:
                    break
        items.append({"source": source, "headline": headline, "link": link, "published_ts": pub})
    return items


def _headline_score(text: str) -> tuple[float, int, list[str]]:
    t = (text or "").lower()
    score = 0.0
    reasons: list[str] = []
    # phrase dictionaries are defined below; tolerate startup order during import
    for phrase, val in globals().get("_BULL_PHRASES", {}).items():
        if phrase in t:
            score += val
            reasons.append(f"bull_phrase:{phrase}")
    for phrase, val in globals().get("_BEAR_PHRASES", {}).items():
        if phrase in t:
            score += val
            reasons.append(f"bear_phrase:{phrase}")
    words = set(re.findall(r"[a-z]+", t))
    bull_hits = words & _BULL_WORDS
    bear_hits = words & _BEAR_WORDS
    if bull_hits:
        score += min(0.30, 0.08 * len(bull_hits))
        reasons.append("bull_words:" + ",".join(sorted(bull_hits)[:4]))
    if bear_hits:
        score -= min(0.30, 0.08 * len(bear_hits))
        reasons.append("bear_words:" + ",".join(sorted(bear_hits)[:4]))
    impact_terms = words & globals().get("_HIGH_IMPACT_TERMS", set())
    impact = 10 + min(90, len(impact_terms) * 18 + (20 if abs(score) >= 0.25 else 0))
    return _clamp(score, -1.0, 1.0) if "_clamp" in globals() else max(-1, min(1, score)), impact, reasons


def _news_relevance(headline: str, instrument: str) -> int:
    t = (headline or "").lower()
    words = set(re.findall(r"[a-z]+", t))
    base = len(words & _RELEVANCE_TERMS) * 12
    inst = instrument.lower()
    if inst in t:
        base += 25
    if instrument.upper() == "NIFTY" and ("nifty" in t or "nse" in t or "indian stock" in t):
        base += 20
    if instrument.upper() == "SENSEX" and ("sensex" in t or "bse" in t or "indian stock" in t):
        base += 20
    return int(max(0, min(100, base)))


def _freshness_score(published_ts: int | None, now_ts: int, max_age: int) -> tuple[int, int | None, bool]:
    """
    News freshness policy for intraday trading confirmation.

    Fixes handled here:
    - RSS feeds sometimes omit pubDate; treat those rows as fetched-now but medium confidence.
    - RSS feeds can produce small future timestamps because of timezone/provider skew.
    - Cached/ancient feed items must be stored only as memory, not used for trading.

    Freshness bands:
    - 0 to 5 minutes: full freshness.
    - 5 to 15 minutes: reduced freshness.
    - Older than 15 minutes: ignored for live signal.
    - Older than 24 hours: rejected as stale/cached.
    """
    if not published_ts:
        return 60, 0, True

    try:
        published_ts = int(published_ts)
    except (TypeError, ValueError):
        return 60, 0, True

    age = now_ts - published_ts

    # Allow small future skew from RSS providers; reject unrealistic future dates.
    if age < 0:
        if abs(age) <= 7200:
            age = 0
        else:
            return 0, age, False

    # Reject old cached feed items.
    if age > 86400:
        return 0, age, False

    # Full freshness for latest market news.
    if age <= 300:
        return 100, age, True

    # Reduced freshness from 5 to 15 minutes.
    if age <= 1800:
        return 35, age, True

    return 0, age, False


def _remember_news(items: list[dict], instrument: str, max_age: int) -> list[dict]:
    _news_db_init()
    now_ts = int(time.time())
    scored: list[dict] = []
    try:
        con = sqlite3.connect(NEWS_DB_FILE)
    except Exception as e:
        logger.warning(f"news db open failed: {e}")
        con = None
    for item in items:
        headline = item["headline"][:500]
        published_ts = item.get("published_ts") or now_ts  # some feeds omit pubdate; treat as fetched-now but low confidence
        sentiment, impact, reasons = _headline_score(headline)
        freshness, age, fresh_ok = _freshness_score(published_ts, now_ts, max_age)
        relevance = _news_relevance(headline, instrument)
        confidence = int(max(0, min(100, abs(sentiment) * 45 + impact * 0.25 + freshness * 0.20 + relevance * 0.20)))
        row = {
            **item,
            "headline": headline,
            "published_ts": int(published_ts),
            "age_seconds": age,
            "is_fresh": fresh_ok,
            "sentiment_score": round(sentiment, 3),
            "impact_score": impact,
            "freshness_score": freshness,
            "relevance_score": relevance,
            "confidence_score": confidence,
            "reasons": reasons[:6],
        }
        scored.append(row)
        if con:
            try:
                nid = hashlib.sha256((item.get("source", "") + headline).encode()).hexdigest()[:32]
                con.execute(
                    """INSERT OR IGNORE INTO news_items
                    (id, source, headline, link, published_ts, fetched_ts, instrument, sentiment_score,
                     impact_score, freshness_score, relevance_score, confidence_score, reasons)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (nid, item.get("source", ""), headline, item.get("link", ""), int(published_ts), now_ts,
                     instrument.upper(), row["sentiment_score"], impact, freshness, relevance, confidence, json.dumps(row["reasons"]))
                )
            except Exception as e:
                logger.debug(f"news remember ignore: {e}")
    if con:
        try:
            # keep db small
            con.execute("DELETE FROM news_items WHERE fetched_ts < ?", (now_ts - 7 * 86400,))
            con.commit(); con.close()
        except Exception:
            pass
    return scored


async def _fetch_free_news(instrument: str, max_age: int) -> tuple[list[dict], list[str]]:
    errs: list[str] = []
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
    # Add targeted Google query per instrument to improve relevance.
    sources = list(_FREE_NEWS_SOURCES)
    key = instrument.upper()
    sources.insert(0, (f"google_{key.lower()}", f"https://news.google.com/rss/search?q={key}%20market%20India%20when:10m&hl=en-IN&gl=IN&ceid=IN:en"))
    all_items: list[dict] = []
    async with httpx.AsyncClient(timeout=8, headers={"User-Agent": ua, "Accept": "application/rss+xml, application/xml, text/xml, */*"}) as c:
        for src, url in sources:
            try:
                r = await c.get(url, follow_redirects=True)
                if r.status_code == 200 and r.text:
                    all_items.extend(_extract_rss_items(r.text, src))
                else:
                    errs.append(f"{src}:HTTP {r.status_code}")
            except httpx.HTTPError as e:
                errs.append(f"{src}:{type(e).__name__}")
    # de-duplicate by normalized headline
    dedup = {}
    for it in all_items:
        k = re.sub(r"\W+", "", it["headline"].lower())[:120]
        dedup.setdefault(k, it)
    return list(dedup.values()), errs


def _aggregate_scored_news(rows: list[dict], require_fresh: bool = True) -> dict:
    usable = [r for r in rows if (r.get("is_fresh") or not require_fresh) and r.get("relevance_score", 0) >= 20]
    weighted_sum = 0.0
    weight_total = 0.0
    for r in usable:
        w = max(1, r.get("impact_score", 0)) * max(1, r.get("freshness_score", 0)) * max(1, r.get("relevance_score", 0)) / 10000
        if abs(r.get("sentiment_score", 0)) > 0:
            weighted_sum += r["sentiment_score"] * w
            weight_total += w
    final = weighted_sum / weight_total if weight_total else 0.0
    coverage = min(100, len(usable) * 20)
    impact = int(max([r.get("impact_score", 0) for r in usable], default=0))
    confidence = int(max(0, min(100, abs(final) * 55 + coverage * 0.25 + impact * 0.20)))
    bias = "BULLISH" if final >= 0.15 else "BEARISH" if final <= -0.15 else "NEUTRAL"
    return {
        "bias": bias,
        "sentiment_score": round(final, 3),
        "confidence": confidence,
        "coverage": coverage,
        "max_impact_score": impact,
        "fresh_count": len([r for r in rows if r.get("is_fresh")]),
        "usable_count": len(usable),
        "items": sorted(usable, key=lambda x: (x.get("confidence_score", 0), x.get("freshness_score", 0)), reverse=True)[:12],
    }


@app.post("/news/sentiment")
async def news_sentiment(req: NewsSentimentRequest):
    key = req.instrument.upper()
    max_age = max(60, min(1800, int(req.max_age_seconds or NEWS_MAX_AGE_SECONDS)))
    now = time.monotonic()
    hit = _news_cache.get(f"{key}:{max_age}")
    if hit and hit[0] > now and not req.force_refresh:
        return {**hit[1], "cached": True}
    raw_items, errs = await _fetch_free_news(key, max_age)
    scored = _remember_news(raw_items, key, max_age)
    agg = _aggregate_scored_news(scored, require_fresh=True)
    payload = {
        "ok": True,
        "source_mode": "free_rss_fresh_only",
        "instrument": key,
        "max_age_seconds": max_age,
        "sentiment_score": agg["sentiment_score"],
        "bias": agg["bias"],
        "confidence": agg["confidence"],
        "coverage": agg["coverage"],
        "impact_score": agg["max_impact_score"],
        "fresh_count": agg["fresh_count"],
        "usable_count": agg["usable_count"],
        "total_seen": len(scored),
        "headlines": [r["headline"] for r in agg["items"]],
        "items": agg["items"],
        "error": "; ".join(errs) if errs and not scored else None,
        "fetched_at": int(time.time()),
        "cached": False,
        "rule": "Only news within max_age_seconds is allowed to affect the signal. Old news is stored but ignored.",
    }
    _news_cache[f"{key}:{max_age}"] = (now + NEWS_TTL, payload)
    return payload


@app.get("/news/memory")
async def news_memory(limit: int = 50, fresh_only: bool = False):
    _news_db_init()
    now_ts = int(time.time())
    try:
        with sqlite3.connect(NEWS_DB_FILE) as con:
            con.row_factory = sqlite3.Row
            where = "WHERE published_ts >= ?" if fresh_only else ""
            params = (now_ts - NEWS_MAX_AGE_SECONDS,) if fresh_only else ()
            rows = con.execute(f"SELECT * FROM news_items {where} ORDER BY fetched_ts DESC LIMIT ?", (*params, max(1, min(200, limit)))).fetchall()
            return {"ok": True, "db": NEWS_DB_FILE, "count": len(rows), "items": [dict(r) for r in rows]}
    except Exception as e:
        return {"ok": False, "error": str(e), "db": NEWS_DB_FILE}

class TelegramNewsRequest(BaseModel):
    channel: str

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
        return {"ok": False, "error": f"HTTP {r.status_code}", "channel": channel}
    raw_msgs = re.findall(
        r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
        r.text, re.DOTALL
    )
    if not raw_msgs:
        return {
            "ok":      False,
            "channel": channel,
            "error":   "no messages parsed",
            "html_size": len(r.text),
        }
    headlines: list[str] = []
    for body in raw_msgs[-30:]:
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
        "headlines":       headlines[-10:],
        "count":           len(headlines),
        "matched":         matched,
        "fetched_at":      int(time.time()),
        "cached":          False,
    }
    _tg_cache[channel] = (now + TG_TTL, payload)
    return payload


# ========================= ADVANCED SIGNAL ENGINE =========================
# Senior-dev note:
# News is treated as a confirmation layer only. The execution decision is based on
# weighted consensus: technicals 60%, news 20%, global/risk context 10%, VIX 10%.
# This prevents the bot from buying options just because a headline looks bullish.

_HIGH_IMPACT_TERMS = {
    "rbi", "fed", "inflation", "cpi", "rate", "rates", "policy", "repo", "war",
    "election", "budget", "crude", "oil", "dollar", "usd", "yield", "yields",
    "fii", "dii", "banknifty", "nifty", "sensex", "global", "gift", "sgx",
}
_BULL_PHRASES = {
    "rate cut": 0.35, "beats estimates": 0.35, "strong earnings": 0.30,
    "record high": 0.25, "fii buying": 0.30, "dii buying": 0.20,
    "crude falls": 0.20, "dollar weakens": 0.15, "inflation cools": 0.25,
    "positive global cues": 0.25, "gift nifty up": 0.20,
}
_BEAR_PHRASES = {
    "rate hike": -0.35, "misses estimates": -0.35, "weak earnings": -0.30,
    "fii selling": -0.30, "crude rises": -0.20, "dollar strengthens": -0.15,
    "inflation rises": -0.25, "negative global cues": -0.25, "gift nifty down": -0.20,
    "war escalates": -0.35, "sell off": -0.35, "selloff": -0.35,
}

class AdvancedSignalRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    min_confidence: int = 60


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _ema(values: list[float], period: int) -> float | None:
    if not values:
        return None
    if len(values) < period:
        return sum(values) / len(values)
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for price in values[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))
    gains = gains[-period:]
    losses = losses[-period:]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _macd(values: list[float]) -> dict:
    if len(values) < 26:
        return {"macd": None, "signal": None, "histogram": None}
    macd_line = (_ema(values, 12) or 0) - (_ema(values, 26) or 0)
    # lightweight signal approximation from rolling macd values
    macd_series = []
    for i in range(26, len(values) + 1):
        sub = values[:i]
        macd_series.append((_ema(sub, 12) or 0) - (_ema(sub, 26) or 0))
    signal = _ema(macd_series, 9) if macd_series else None
    hist = macd_line - signal if signal is not None else None
    return {"macd": round(macd_line, 4), "signal": round(signal, 4) if signal is not None else None, "histogram": round(hist, 4) if hist is not None else None}



# Legacy headline aggregation is intentionally bypassed. The production news layer
# above already handles freshness, relevance, impact, confidence and SQLite memory.
async def _advanced_news_context(instrument: str) -> dict:
    base = await news_sentiment(NewsSentimentRequest(instrument=instrument, max_age_seconds=NEWS_MAX_AGE_SECONDS))
    bias = base.get("bias", "NEUTRAL")
    return {
        "bias": bias,
        "sentiment_score": float(base.get("sentiment_score") or 0),
        "confidence": int(base.get("confidence") or 0),
        "coverage": int(base.get("coverage") or 0),
        "impact_score": int(base.get("impact_score") or 0),
        "fresh_count": int(base.get("fresh_count") or 0),
        "usable_count": int(base.get("usable_count") or 0),
        "headlines": list(base.get("headlines") or [])[:12],
        "scored_items": list(base.get("items") or [])[:12],
        "rss_error": base.get("error"),
        "source": "free_rss_sqlite_fresh_only_no_telegram_no_claude",
        "rule": base.get("rule"),
    }


def _technical_from_market(md: dict, prices: list[float]) -> dict:
    ltp = float(md.get("ltp") or 0)
    open_ = float(md.get("open") or ltp or 0)
    high = float(md.get("high") or ltp or 0)
    low = float(md.get("low") or ltp or 0)
    vix = float(md.get("vix") or 15)
    change = float(md.get("change") or 0)
    momentum_5m = float(md.get("momentum_5m") or 0)
    if ltp > 0 and (not prices or prices[-1] != ltp):
        prices = prices + [ltp]
    ema20 = _ema(prices, 20) if prices else None
    ema50 = _ema(prices, 50) if prices else None
    rsi14 = _rsi(prices, 14) if prices else None
    macd = _macd(prices) if prices else {"macd": None, "signal": None, "histogram": None}
    day_range = max(high - low, 0.01)
    vwap_proxy = (high + low + ltp) / 3 if ltp else 0

    score = 0.0
    reasons: list[str] = []
    if ltp and ema20:
        if ltp > ema20:
            score += 0.18; reasons.append("price_above_ema20")
        else:
            score -= 0.18; reasons.append("price_below_ema20")
    if ema20 and ema50:
        if ema20 > ema50:
            score += 0.16; reasons.append("ema20_above_ema50")
        else:
            score -= 0.16; reasons.append("ema20_below_ema50")
    if rsi14 is not None:
        if 55 <= rsi14 <= 70:
            score += 0.16; reasons.append("rsi_bullish_not_overbought")
        elif 30 <= rsi14 <= 45:
            score -= 0.16; reasons.append("rsi_bearish_not_oversold")
        elif rsi14 > 78:
            score -= 0.10; reasons.append("rsi_overbought")
        elif rsi14 < 22:
            score += 0.10; reasons.append("rsi_oversold_bounce_zone")
    if macd.get("histogram") is not None:
        if macd["histogram"] > 0:
            score += 0.14; reasons.append("macd_positive")
        else:
            score -= 0.14; reasons.append("macd_negative")
    if ltp > vwap_proxy:
        score += 0.10; reasons.append("price_above_vwap_proxy")
    elif ltp:
        score -= 0.10; reasons.append("price_below_vwap_proxy")
    # opening range / intraday breakout proxy
    if ltp >= high - day_range * 0.15 and change > 0:
        score += 0.14; reasons.append("near_day_high_breakout")
    if ltp <= low + day_range * 0.15 and change < 0:
        score -= 0.14; reasons.append("near_day_low_breakdown")
    if momentum_5m > 0.08:
        score += 0.10; reasons.append("positive_5m_momentum")
    elif momentum_5m < -0.08:
        score -= 0.10; reasons.append("negative_5m_momentum")

    score = _clamp(score, -1.0, 1.0)
    bias = "BULLISH" if score >= 0.25 else "BEARISH" if score <= -0.25 else "NEUTRAL"
    data_quality = min(100, 25 + len(prices) * 3)
    confidence = int(_clamp(abs(score) * 100 * 0.75 + data_quality * 0.25, 0, 100))
    return {
        "bias": bias,
        "technical_score": round(score, 3),
        "confidence": confidence,
        "data_points": len(prices),
        "ltp": ltp,
        "open": open_, "high": high, "low": low, "vwap_proxy": round(vwap_proxy, 2),
        "change": change, "momentum_5m": momentum_5m, "vix": vix,
        "ema20": round(ema20, 2) if ema20 else None,
        "ema50": round(ema50, 2) if ema50 else None,
        "rsi14": round(rsi14, 2) if rsi14 is not None else None,
        "macd": macd,
        "reasons": reasons,
    }


def _market_regime_score(md: dict, technical: dict) -> dict:
    vix = float(md.get("vix") or 15)
    change = float(md.get("change") or 0)
    score = 0.0
    reasons = []
    if vix >= 22:
        score -= 0.25; reasons.append("high_vix_avoid_option_buying")
    elif vix <= 13:
        score += 0.10; reasons.append("low_vix_stable")
    else:
        reasons.append("normal_vix")
    if abs(change) >= 0.35:
        score += 0.10 if change > 0 else -0.10
        reasons.append("index_direction_confirmed")
    return {"score": round(_clamp(score, -1, 1), 3), "vix": vix, "reasons": reasons}


@app.post("/signals/advanced")
async def advanced_signal(req: AdvancedSignalRequest):
    # Market data fetch also records ticks, which improves indicators over time.
    md = await get_market_data(QuoteRequest(session_id=req.session_id, instrument=req.instrument))
    if not md.get("success"):
        return {"success": False, "error": "market data failed", "market_data": md}
    try:
        prices = tick_recorder.prices(req.instrument.upper(), limit=120)  # added in ticks.py
    except Exception:
        prices = []
    technical = _technical_from_market(md, prices)
    news = await _advanced_news_context(req.instrument)
    regime = _market_regime_score(md, technical)

    # Dynamic weighting:
    # News is confirmation only. If there is no fresh/usable news, do not penalize
    # an otherwise valid technical setup.
    if int(news.get("usable_count") or 0) == 0:
        composite = (
            technical["technical_score"] * 0.80 +
            regime["score"] * 0.10 +
            (0.10 if technical.get("vix", 15) < 20 else -0.10) * 0.10
        )
        confidence = int(_clamp(
            technical["confidence"] * 0.80 +
            abs(regime["score"]) * 100 * 0.10 +
            10,
            0, 100
        ))
    else:
        composite = (
            technical["technical_score"] * 0.60 +
            news["sentiment_score"] * 0.20 +
            regime["score"] * 0.10 +
            (0.10 if technical.get("vix", 15) < 20 else -0.10) * 0.10
        )
        confidence = int(_clamp(
            technical["confidence"] * 0.60 +
            news["confidence"] * 0.20 +
            abs(regime["score"]) * 100 * 0.10 +
            10,
            0, 100
        ))

    composite = round(_clamp(composite, -1.0, 1.0), 3)
    if confidence < req.min_confidence or abs(composite) < 0.22:
        action = "WAIT"
        option_side = None
    elif composite > 0:
        action = "BUY_CE"
        option_side = "CE"
    else:
        action = "BUY_PE"
        option_side = "PE"

    risk_ok, risk_reason = risk_engine.can_trade(0)
    if not risk_ok:
        action = "BLOCKED_BY_RISK"
        option_side = None

    return {
        "success": True,
        "instrument": req.instrument.upper(),
        "action": action,
        "option_side": option_side,
        "confidence": confidence,
        "composite_score": composite,
        "min_confidence": req.min_confidence,
        "weights": (
            {"technical": 0.80, "news": 0.00, "market_regime": 0.10, "vix": 0.10}
            if int(news.get("usable_count") or 0) == 0
            else {"technical": 0.60, "news": 0.20, "market_regime": 0.10, "vix": 0.10}
        ),
        "technical": technical,
        "news": news,
        "market_regime": regime,
        "risk": {"allowed": risk_ok, "reason": risk_reason, "state": risk_engine.state()},
        "market_data": {k: md.get(k) for k in ("ltp", "open", "high", "low", "change", "vix", "momentum_5m", "source")},
        "execution_note": "Use this signal only as pre-trade confirmation. Keep separate order placement and stop-loss logic active.",
    }

@app.post("/news/advanced")
async def advanced_news(req: AdvancedSignalRequest):
    return await _advanced_news_context(req.instrument)

# ======================= END ADVANCED SIGNAL ENGINE =======================

@app.post("/quotes/stream_status")
async def stream_status(req: SessionRequest):
    s = streamers.get(req.session_id)
    if s is None:
        return {"running": False, "message": "no streamer for this session"}
    return {"running": True, **s.status()}

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

class ChainRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    spot:       float = 0.0
    n_strikes:  int = 5

@app.post("/chain/quotes")
async def chain_quotes_legacy(req: ChainRequest):
    req2 = ChainRequest(
        session_id=req.session_id, instrument=req.instrument,
        spot=req.spot or 0, n_strikes=req.n_strikes,
    )
    return await chain_atm(req2)

def _fo_segment(instrument: str) -> str:
    return "bse_fo" if instrument.upper() == "SENSEX" else "nse_fo"

@app.post("/chain/atm")
async def chain_atm(req: ChainRequest):
    sess = get_session(req.session_id)
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")
    spot = req.spot
    if spot <= 0:
        async with httpx.AsyncClient(timeout=10) as c:
            ltp, err = await _ltp_via_script_details(c, sess, inst["neo_symbol"], inst["exchange_segment"])
            if ltp is None or ltp <= 0:
                return {"success": False, "error": f"spot fetch failed: {err}", "instrument": req.instrument}
            spot = ltp
    try:
        chain_meta = await scrip_master.find_atm_chain(sess, req.instrument, spot, n=req.n_strikes)
    except Exception as e:
        logger.error(f"chain/atm scrip master: {e}")
        return {"success": False, "error": f"scrip master: {e}", "instrument": req.instrument, "spot": spot}
    if not chain_meta["strikes"]:
        return {
            "success": False,
            "error": "no option strikes resolved",
            "instrument": req.instrument, "spot": spot,
            "scrip_status": "ok",
        }
    legs: list[tuple[str, str, dict]] = []
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
                await asyncio.sleep(1.0)
    return {
        "success":    True,
        "instrument": req.instrument,
        "spot":       spot,
        "atm":        chain_meta["atm"],
        "expiry":     chain_meta["expiry"],
        "step":       chain_meta["step"],
        "strikes":    [out_strikes[r["strike"]] for r in chain_meta["strikes"]],
    }

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
    cached = sorted(scrip_master._cache.keys())
    return {
        "cached_segments_today": [seg for seg, _ in cached],
        "cache_keys":            [{"segment": s, "date": d, "rows": len(scrip_master._cache[(s, d)])} for s, d in cached],
        "paths_cached":          scrip_master._paths_cache[0] if scrip_master._paths_cache else None,
    }

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
    if not rl.check_order():
        raise HTTPException(
            status_code=429,
            detail=f"Order rate limit exceeded. Status: {rl.status()['order_api']}"
        )
    sess = get_session(req.session_id)
    jData = {"am": req.amo, "dq": req.disclosed_quantity, "es": req.exchange_segment,
             "mp": req.market_protection, "pc": req.product, "pf": req.pf,
             "pr": req.price, "pt": req.order_type, "qt": req.quantity,
             "rt": req.validity, "tp": req.trigger_price, "ts": req.trading_symbol, "tt": req.transaction_type}
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
                data={"jData": json.dumps(jData)})
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
                data={"jData": json.dumps({"am": req.am, "on": req.order_no})})
            data, err = safe_json(r)
            if err:
                return {"success": False, "error": err}
            return data
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

class OptionFullLeg(BaseModel):
    p_symbol: str
    exchange: str = "nse_fo"

class OptionFullRequest(BaseModel):
    session_id: str
    legs: list[OptionFullLeg]

@app.post("/quotes/option_full")
async def option_full(req: OptionFullRequest):
    """Return LTP/OI/volume for option legs. Fixes UI 404 and gives the
    technical panel OI/volume inputs. Uses Kotak script-details endpoint.
    """
    sess = get_session(req.session_id)
    out = []
    async with httpx.AsyncClient(timeout=12) as c:
        for leg in req.legs[:20]:
            item, err = await _get_quote_item(c, sess, leg.p_symbol, leg.exchange)
            if not item:
                out.append({"ok": False, "p_symbol": leg.p_symbol, "exchange": leg.exchange, "error": err})
                continue
            def pick(*keys):
                ci = {str(k).lower(): k for k in item.keys()}
                for k in keys:
                    rk = ci.get(k.lower())
                    if rk is not None and item.get(rk) not in (None, ""):
                        return item.get(rk)
                return None
            def num(v):
                try:
                    return float(str(v).replace(",", ""))
                except Exception:
                    return 0.0
            out.append({
                "ok": True,
                "p_symbol": leg.p_symbol,
                "exchange": leg.exchange,
                "ltp": num(pick("ltp", "last_price", "lastPrice")),
                "oi": num(pick("oi", "openInterest", "open_interest")),
                "vol": num(pick("volume", "vol", "v", "tradeVolume")),
                "bid": num(pick("bid", "bp", "bestBid")),
                "ask": num(pick("ask", "sp", "bestAsk")),
                "raw": item,
            })
    return {"success": True, "legs": out}

class AffordableChainRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    side: str = "CE"                 # CE / PE
    n_strikes: int = 60              # deep scan to find affordable low-premium OTM contracts
    max_deploy_pct: float = 95.0     # percent of available wallet allowed for premium debit
    min_option_ltp: float = 0.05
    spot: float = 0.0

def _ist_now():
    return datetime.now(ZoneInfo("Asia/Kolkata"))

def _entry_window_status(bypass: bool = False) -> dict:
    if bypass:
        return {"allowed": True, "reason": "bypassed"}
    now = _ist_now()
    mins = now.hour * 60 + now.minute
    if now.weekday() >= 5:
        return {"allowed": False, "reason": "weekend - market closed", "ist": now.isoformat()}
    if mins < 9 * 60 + 30:      
        return {"allowed": False, "reason": "no entry before 09:30 IST", "ist": now.isoformat()}
    if mins >= 15 * 60 + 15:
        return {"allowed": False, "reason": "no entry after 15:15 IST", "ist": now.isoformat()}
    return {"allowed": True, "reason": "entry window ok", "ist": now.isoformat()}

def _expiry_is_today(expiry) -> bool:
    if not expiry:
        return False
    s = str(expiry).strip().upper()
    today = _ist_now().date()
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d%b%Y", "%d-%b-%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date() == today
        except Exception:
            pass
    return today.strftime("%d%b%Y").upper() in s

async def _refresh_wallet_for_session(session_id: str) -> float:
    try:
        w = await limits(SessionRequest(session_id=session_id))
        for k in ("avlMrgn", "avlCash"):
            if w.get(k) not in (None, ""):
                return float(w.get(k))
    except Exception as e:
        logger.warning(f"wallet refresh failed: {e}")
    return float(risk_engine.state().get("wallet") or 0)

def _leg_sort_key(row: dict, side: str, atm: float) -> tuple:
    strike = float(row.get("strike") or 0)
    # For CE, prefer ATM then OTM above spot; for PE, prefer ATM then OTM below spot.
    if side == "CE":
        wrong_side_penalty = 100000 if strike < atm else 0
    else:
        wrong_side_penalty = 100000 if strike > atm else 0
    return (wrong_side_penalty + abs(strike - atm), strike)

async def _select_affordable_option(
    session_id: str,
    instrument: str,
    side: str,
    *,
    n_strikes: int = 60,
    max_deploy_pct: float = 95.0,
    min_option_ltp: float = 0.05,
    spot: float = 0.0,
) -> dict:
    """Wallet-aware option selector. It does NOT blindly pick ATM.

    Selection rule:
    1. Resolve a deep option chain.
    2. Quote only the requested side.
    3. Filter contracts where one lot premium debit fits inside wallet budget.
    4. Pick the nearest-to-ATM affordable OTM/ATM contract, not the cheapest lottery contract.
    """
    instrument = instrument.upper()
    side = side.upper()
    if side not in ("CE", "PE"):
        return {"success": False, "error": "side must be CE or PE"}
    sess = get_session(session_id)
    inst = INSTRUMENT_TOKENS.get(instrument)
    if not inst:
        return {"success": False, "error": f"Unknown instrument: {instrument}"}
    wallet = await _refresh_wallet_for_session(session_id)
    if wallet <= 0:
        return {"success": False, "error": "wallet is zero; refresh wallet/login again", "wallet": wallet}
    if spot <= 0:
        async with httpx.AsyncClient(timeout=10) as c:
            spot_ltp, err = await _ltp_via_script_details(c, sess, inst["neo_symbol"], inst["exchange_segment"])
        if not spot_ltp or spot_ltp <= 0:
            return {"success": False, "stage": "spot_fetch", "error": err or "spot is 0", "wallet": wallet}
        spot = spot_ltp
    try:
        chain_meta = await scrip_master.find_atm_chain(sess, instrument, spot, n=max(4, min(90, n_strikes)))
    except Exception as e:
        return {"success": False, "stage": "scrip_master", "error": str(e), "spot": spot, "wallet": wallet}
    expiry = chain_meta.get("expiry")
    fo_seg = _fo_segment(instrument)
    atm = float(chain_meta.get("atm") or 0)
    side_key = side.lower()
    budget = wallet * (_clamp(max_deploy_pct, 1, 100) / 100.0)
    raw_rows = sorted(chain_meta.get("strikes") or [], key=lambda r: _leg_sort_key(r, side, atm))
    candidates = []
    async with httpx.AsyncClient(timeout=12) as c:
        for row in raw_rows:
            leg = row.get(side_key)
            if not leg or not leg.get("p_symbol"):
                continue
            ltp, err = await _ltp_via_script_details(c, sess, leg["p_symbol"], fo_seg)
            lot_size = int(leg.get("lot_size") or (20 if instrument == "SENSEX" else 75))
            ltp = float(ltp or 0)
            per_lot_cost = round(ltp * lot_size, 2) if ltp > 0 else 0.0
            affordable = bool(ltp >= min_option_ltp and per_lot_cost > 0 and per_lot_cost <= budget)
            candidates.append({
                "strike": float(row.get("strike")),
                "atm": float(row.get("strike")) == atm,
                "side": side,
                "p_symbol": leg.get("p_symbol"),
                "p_trd_symbol": leg.get("p_trd_symbol"),
                "lot_size": lot_size,
                "ltp": ltp,
                "per_lot_cost": per_lot_cost,
                "affordable": affordable,
                "distance_from_atm": abs(float(row.get("strike") or 0) - atm),
                "error": err,
            })
            await asyncio.sleep(0.05)
    affordable = [c for c in candidates if c["affordable"]]
    if not affordable:
        cheapest = min([c for c in candidates if c["per_lot_cost"] > 0], key=lambda x: x["per_lot_cost"], default=None)
        return {
            "success": False,
            "error": "no affordable option found in scanned chain",
            "instrument": instrument, "side": side, "wallet": wallet, "budget": round(budget, 2),
            "spot": spot, "atm": atm, "expiry": expiry, "fo_segment": fo_seg,
            "cheapest_seen": cheapest,
            "candidates": candidates[:30],
        }
    # Production selector: choose the minimum premium that still looks tradable,
    # then prefer the nearer strike. This matches small-wallet option buying.
    # Extreme zero/liquidity-trap contracts are filtered by min_option_ltp.
    affordable.sort(key=lambda c: (c["ltp"], c["distance_from_atm"]))
    selected = affordable[0]
    lots = int(budget // selected["per_lot_cost"]) if selected["per_lot_cost"] > 0 else 0
    lots = max(1, lots)
    selected["lots"] = lots
    selected["qty"] = lots * selected["lot_size"]
    selected["estimated_cost"] = round(selected["qty"] * selected["ltp"], 2)
    return {
        "success": True,
        "instrument": instrument, "side": side, "wallet": wallet, "budget": round(budget, 2),
        "spot": spot, "atm": atm, "expiry": expiry, "fo_segment": fo_seg,
        "selected": selected,
        "candidates": candidates[:30],
        "selection_rule": "minimum affordable tradable premium within wallet budget",
    }

@app.post("/chain/affordable")
async def chain_affordable(req: AffordableChainRequest):
    return await _select_affordable_option(
        req.session_id, req.instrument, req.side,
        n_strikes=req.n_strikes, max_deploy_pct=req.max_deploy_pct,
        min_option_ltp=req.min_option_ltp, spot=req.spot,
    )

class TradeExecuteRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    min_confidence: int = 60
    force_side: str = "AUTO"          # AUTO / CE / PE
    dry_run: bool = False             # production live by default; UI has explicit dry-run toggle
    bypass_time_check: bool = False
    allow_expiry_day: bool = False
    max_deploy_pct: float = 95.0
    n_strikes: int = 60
    min_option_ltp: float = 0.05
    order_type: str = "L"             # L recommended; MKT only if allow_market_order true
    allow_market_order: bool = False
    max_slippage_pct: float = 2.0      # buy limit = ltp * (1 + this%)
    product: str = "NRML"

async def _execute_trade_plan(req: TradeExecuteRequest) -> dict:
    window = _entry_window_status(req.bypass_time_check)
    if not window["allowed"]:
        return {"success": False, "stage": "time_window", "error": window["reason"], "window": window}
    sig = await advanced_signal(AdvancedSignalRequest(
        session_id=req.session_id, instrument=req.instrument, min_confidence=req.min_confidence,
    ))
    if not sig.get("success"):
        return {"success": False, "stage": "signal", "error": sig.get("error"), "signal": sig}
    forced = req.force_side.upper().strip()
    side = None
    if forced in ("CE", "PE"):
        side = forced
    elif sig.get("action") == "BUY_CE":
        side = "CE"
    elif sig.get("action") == "BUY_PE":
        side = "PE"
    else:
        return {"success": False, "stage": "signal", "error": f"signal action is {sig.get('action')} - not tradable", "signal": sig}
    chain = await _select_affordable_option(
        req.session_id, req.instrument, side, n_strikes=req.n_strikes,
        max_deploy_pct=req.max_deploy_pct, min_option_ltp=req.min_option_ltp,
    )
    if not chain.get("success"):
        return {"success": False, "stage": "affordable_chain", "error": chain.get("error"), "signal": sig, "chain": chain}
    if _expiry_is_today(chain.get("expiry")) and not req.allow_expiry_day:
        return {"success": False, "stage": "expiry_guard", "error": "expiry day blocked", "signal": sig, "chain": chain}
    sel = chain["selected"]
    required = float(sel.get("estimated_cost") or 0)
    risk_ok, risk_reason = risk_engine.can_trade(required)
    if not risk_ok:
        return {"success": False, "stage": "risk", "error": risk_reason, "signal": sig, "chain": chain, "risk": risk_engine.state()}
    if req.order_type.upper() == "MKT" and not req.allow_market_order:
        return {"success": False, "stage": "order_guard", "error": "MKT blocked unless allow_market_order=true"}
    buy_price = max(0.05, round(sel["ltp"] * (1 + max(0, req.max_slippage_pct) / 100.0), 2))
    order_payload = {
        "session_id": req.session_id,
        "trading_symbol": sel["p_trd_symbol"],
        "transaction_type": "B",
        "quantity": str(sel["qty"]),
        "order_type": req.order_type.upper(),
        "price": f"{buy_price:.2f}" if req.order_type.upper() == "L" else "0",
        "product": req.product,
        "exchange_segment": chain["fo_segment"],
    }
    plan = {
        "signal": sig, "chain": chain, "selected": sel, "order_payload": order_payload,
        "guards": {"time_window": window, "risk": {"allowed": risk_ok, "reason": risk_reason}},
        "dry_run": req.dry_run,
    }
    if req.dry_run:
        return {"success": True, "stage": "dry_run", "message": "trade plan ready; no order placed", **plan}
    res = await place_order(OrderRequest(**order_payload))
    accepted = isinstance(res, dict) and res.get("stat") == "Ok"
    return {"success": accepted, "stage": "live_order", "order_result": res, **plan}

@app.post("/trade/execute")
async def trade_execute(req: TradeExecuteRequest):
    return await _execute_trade_plan(req)

class TestPlaceRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    price: float = 0.05
    qty_lots: int = 1
    side: str = "AUTO"
    max_deploy_pct: float = 95.0
    n_strikes: int = 60

@app.post("/orders/test_place")
async def test_place(req: TestPlaceRequest):
    """Connectivity test: choose wallet-affordable strike, place a low-limit BUY,
    then cancel immediately. This no longer blindly selects ATM CE.
    """
    side = req.side.upper().strip()
    if side == "AUTO":
        # For a test order we do not need a directional signal; use CE as broker-connectivity test.
        side = "CE"
    chain = await _select_affordable_option(
        req.session_id, req.instrument, side, n_strikes=req.n_strikes,
        max_deploy_pct=req.max_deploy_pct, min_option_ltp=0.05,
    )
    if not chain.get("success"):
        return {"success": False, "stage": "affordable_chain", "error": chain.get("error"), "chain": chain}
    sess = get_session(req.session_id)
    sel = chain["selected"]
    # Test order limit deliberately below LTP to reduce fill probability, then cancel.
    safe_limit = max(0.05, round(float(sel["ltp"]) * 0.80, 2))
    if req.price and req.price > 0:
        safe_limit = min(safe_limit, float(req.price))
    qty = max(1, int(req.qty_lots)) * int(sel["lot_size"])
    place_url = f"{sess['base_url']}/quick/order/rule/ms/place"
    jData = {
        "am": "NO", "dq": "0", "es": chain["fo_segment"], "mp": "0",
        "pc": "NRML", "pf": "N", "pr": f"{safe_limit:.2f}", "pt": "L",
        "qt": str(qty), "rt": "DAY", "tp": "0", "ts": sel["p_trd_symbol"], "tt": "B",
    }
    place_diag = {"url": place_url, "jData": jData, "selected": sel, "chain_summary": {k: chain.get(k) for k in ("wallet", "budget", "spot", "atm", "expiry")}}
    async with httpx.AsyncClient(timeout=20) as c:
        try:
            r = await c.post(place_url, headers={"accept": "application/json", "Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/x-www-form-urlencoded"}, data={"jData": json.dumps(jData)})
        except httpx.HTTPError as e:
            return {"success": False, "stage": "place_http", "error": f"{type(e).__name__}: {e}", "place": place_diag}
    body, parse_err = safe_json(r)
    place_diag.update({"http_status": r.status_code, "body_snippet": (r.text or "")[:600], "body": body, "parse_error": parse_err})
    order_no = body.get("nOrdNo") if isinstance(body, dict) else None
    cancel_diag = {}
    if order_no:
        async with httpx.AsyncClient(timeout=15) as c:
            try:
                cr = await c.post(f"{sess['base_url']}/quick/order/cancel", headers={"accept": "application/json", "Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/x-www-form-urlencoded"}, data={"jData": json.dumps({"am": "NO", "on": str(order_no)})})
                cb, cerr = safe_json(cr)
                cancel_diag = {"http_status": cr.status_code, "body": cb, "parse_error": cerr}
            except httpx.HTTPError as e:
                cancel_diag = {"transport_error": f"{type(e).__name__}: {e}"}
    rejected_reason = body.get("emsg") or body.get("message") if isinstance(body, dict) else None
    return {
        "success": bool(order_no),
        "verdict": "accepted (and cancelled)" if order_no else "rejected",
        "rejected_reason": rejected_reason,
        "place": place_diag,
        "cancel": cancel_diag,
        "note": "Test order uses affordable strike selection, not fixed ATM CE.",
    }

class ResolveStrikeRequest(BaseModel):
    session_id: str
    instrument: str
    strike:     float
    side:       str

@app.post("/chain/resolve_strike")
async def resolve_strike(req: ResolveStrikeRequest):
    sess = get_session(req.session_id)
    inst = INSTRUMENT_TOKENS.get(req.instrument.upper())
    if not inst:
        raise HTTPException(status_code=400, detail=f"Unknown instrument: {req.instrument}")
    side = req.side.upper()
    if side not in ("CE", "PE"):
        raise HTTPException(status_code=400, detail="side must be CE or PE")
    async with httpx.AsyncClient(timeout=10) as c:
        spot, err = await _ltp_via_script_details(c, sess, inst["neo_symbol"], inst["exchange_segment"])
        if not spot or spot <= 0:
            return {"ok": False, "error": f"spot fetch failed: {err}"}
    step = 100 if req.instrument.upper() in ("BANKNIFTY", "SENSEX") else 50
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
