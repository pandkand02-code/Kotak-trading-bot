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

BOT_HTML = open("bot.html").read() if os.path.exists("bot.html") else "<h1>bot.html missing</h1>"

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    return HTMLResponse(content=BOT_HTML)

@app.get("/health")
async def health():
    return {"status": "NEXUS v3 Running", "version": "3.0.0", "time": datetime.now().isoformat()}

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
_tg_cache: dict[str, tuple[float, dict]] = {}
TG_TTL = 180
NEWS_TTL = 300

class NewsSentimentRequest(BaseModel):
    instrument: str = "NIFTY"

@app.post("/news/sentiment")
async def news_sentiment(req: NewsSentimentRequest):
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
                        break
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
        "sentiment_score": sentiment,
        "headlines":       headlines[:8],
        "count":           len(headlines),
        "matched":         matched,
        "error":           err,
        "fetched_at":      int(time.time()),
        "cached":          False,
    }
    _news_cache[key] = (now + NEWS_TTL, payload)
    return payload

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

class TestPlaceRequest(BaseModel):
    session_id: str
    instrument: str = "NIFTY"
    price:      float = 0.05
    qty_lots:   int   = 1

@app.post("/orders/test_place")
async def test_place(req: TestPlaceRequest):
    sess = get_session(req.session_id)
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
    async with httpx.AsyncClient(timeout=10) as c:
        opt_ltp, _ = await _ltp_via_script_details(c, sess, pSymbol, fo_seg)
    safe_limit = req.price
    if opt_ltp and opt_ltp > 0:
        safe_limit = max(0.05, round(opt_ltp * 0.95, 2))
    place_url = f"{sess['base_url']}/quick/order/rule/ms/place"
    jData = {
        "am": "NO", "dq": "0", "es": fo_seg, "mp": "0",
        "pc": "NRML", "pf": "N",
        "pr": f"{safe_limit:.2f}",
        "pt": "L",
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
