"""
NEXUS Trading Bot — FastAPI Backend
Handles all Kotak NEO API calls server-side (no CORS issues)
Deploy on Railway.app or Render.com (free tier)
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import json
import os
from datetime import datetime
from pydantic import BaseModel
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NEXUS Trading Bot API", version="1.0.0")

# ── CORS: allow your mobile browser to call this backend ──
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten to your domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Kotak base URLs ──
KOTAK_LOGIN_URL    = "https://mis.kotaksecurities.com/login/1.0/tradeApiLogin"
KOTAK_VALIDATE_URL = "https://mis.kotaksecurities.com/login/1.0/tradeApiValidate"
NEO_FIN_KEY        = "neotradeapi"

# ── In-memory session store (single user; extend with Redis for multi-user) ──
sessions: dict = {}

# ─────────────────────────────────────────────────────────────────────────────
# REQUEST MODELS
# ─────────────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    access_token: str
    mobile: str
    ucc: str
    totp: str

class ValidateRequest(BaseModel):
    access_token: str
    view_token: str
    view_sid: str
    mpin: str

class LimitsRequest(BaseModel):
    session_id: str

class OrderRequest(BaseModel):
    session_id: str
    exchange_segment: str = "nse_fo"
    trading_symbol: str
    transaction_type: str        # B or S
    quantity: str
    order_type: str = "MKT"      # MKT or L
    price: str = "0"
    product: str = "NRML"
    validity: str = "DAY"
    amo: str = "NO"
    disclosed_quantity: str = "0"
    market_protection: str = "0"
    pf: str = "N"
    trigger_price: str = "0"

class CancelRequest(BaseModel):
    session_id: str
    order_no: str
    am: str = "NO"

class ModifyRequest(BaseModel):
    session_id: str
    order_no: str
    trading_symbol: str
    quantity: str
    price: str
    order_type: str
    validity: str = "DAY"
    product: str = "NRML"
    exchange_segment: str = "nse_fo"
    market_protection: str = "0"
    disclosed_quantity: str = "0"
    trigger_price: str = "0"

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_session(session_id: str) -> dict:
    if session_id not in sessions:
        raise HTTPException(status_code=401, detail="Session not found. Please authenticate again.")
    return sessions[session_id]

def log_request(endpoint: str, payload: dict = None):
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] {endpoint} | {payload or ''}")

# ─────────────────────────────────────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "status": "NEXUS Backend Running",
        "version": "1.0.0",
        "time": datetime.now().isoformat(),
        "endpoints": ["/auth/login", "/auth/validate", "/wallet/limits",
                      "/orders/place", "/orders/list", "/orders/cancel",
                      "/positions", "/chain/{instrument}"]
    }

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# ─────────────────────────────────────────────────────────────────────────────
# AUTH — STEP 1: TOTP LOGIN
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/auth/login")
async def login(req: LoginRequest):
    log_request("/auth/login", {"mobile": req.mobile, "ucc": req.ucc})
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.post(
                KOTAK_LOGIN_URL,
                headers={
                    "Authorization": req.access_token,
                    "neo-fin-key": NEO_FIN_KEY,
                    "Content-Type": "application/json",
                },
                json={"mobileNumber": req.mobile, "ucc": req.ucc, "totp": req.totp},
            )
            data = r.json()
            logger.info(f"Kotak login response status: {r.status_code}")
            if data.get("data", {}).get("token"):
                return {
                    "success": True,
                    "view_token": data["data"]["token"],
                    "view_sid": data["data"]["sid"],
                    "message": "TOTP login successful"
                }
            return {"success": False, "message": data.get("message", "Login failed"), "raw": data}
        except Exception as e:
            logger.error(f"Login error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# AUTH — STEP 2: MPIN VALIDATE
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/auth/validate")
async def validate(req: ValidateRequest):
    log_request("/auth/validate")
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.post(
                KOTAK_VALIDATE_URL,
                headers={
                    "Authorization": req.access_token,
                    "neo-fin-key": NEO_FIN_KEY,
                    "sid": req.view_sid,
                    "Auth": req.view_token,
                    "Content-Type": "application/json",
                },
                json={"mpin": req.mpin},
            )
            data = r.json()
            if data.get("data", {}).get("token"):
                # Store session server-side
                session_id = f"sess_{req.view_sid[-8:]}"
                sessions[session_id] = {
                    "session_token": data["data"]["token"],
                    "session_sid":   data["data"]["sid"],
                    "base_url":      data["data"].get("baseUrl", "https://cis.kotaksecurities.com"),
                    "created_at":    datetime.now().isoformat(),
                }
                return {
                    "success": True,
                    "session_id": session_id,
                    "base_url": sessions[session_id]["base_url"],
                    "message": "Authentication complete"
                }
            return {"success": False, "message": data.get("message", "MPIN validation failed"), "raw": data}
        except Exception as e:
            logger.error(f"Validate error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# WALLET / LIMITS
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/wallet/limits")
async def get_limits(req: LimitsRequest):
    sess = get_session(req.session_id)
    log_request("/wallet/limits")
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.post(
                f"{sess['base_url']}/quick/user/limits",
                headers={
                    "Auth": sess["session_token"],
                    "Sid":  sess["session_sid"],
                    "neo-fin-key": NEO_FIN_KEY,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"jData": json.dumps({"seg": "ALL", "exch": "ALL", "prod": "ALL"})},
            )
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# PLACE ORDER
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/orders/place")
async def place_order(req: OrderRequest):
    sess = get_session(req.session_id)
    log_request("/orders/place", {"symbol": req.trading_symbol, "qty": req.quantity, "type": req.transaction_type})
    jData = {
        "am":  req.amo,
        "dq":  req.disclosed_quantity,
        "es":  req.exchange_segment,
        "mp":  req.market_protection,
        "pc":  req.product,
        "pf":  req.pf,
        "pr":  req.price,
        "pt":  req.order_type,
        "qt":  req.quantity,
        "rt":  req.validity,
        "tp":  req.trigger_price,
        "ts":  req.trading_symbol,
        "tt":  req.transaction_type,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.post(
                f"{sess['base_url']}/quick/order/rule/ms/place",
                headers={
                    "Auth": sess["session_token"],
                    "Sid":  sess["session_sid"],
                    "neo-fin-key": NEO_FIN_KEY,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"jData": json.dumps(jData)},
            )
            result = r.json()
            logger.info(f"Order result: {result}")
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# CANCEL ORDER
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/orders/cancel")
async def cancel_order(req: CancelRequest):
    sess = get_session(req.session_id)
    log_request("/orders/cancel", {"order_no": req.order_no})
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.post(
                f"{sess['base_url']}/quick/order/cancel",
                headers={
                    "Auth": sess["session_token"],
                    "Sid":  sess["session_sid"],
                    "neo-fin-key": NEO_FIN_KEY,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"jData": json.dumps({"am": req.am, "on": req.order_no})},
            )
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# MODIFY ORDER
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/orders/modify")
async def modify_order(req: ModifyRequest):
    sess = get_session(req.session_id)
    jData = {
        "dq": req.disclosed_quantity, "es": req.exchange_segment,
        "mp": req.market_protection,  "on": req.order_no,
        "pc": req.product,            "pr": req.price,
        "pt": req.order_type,         "qt": req.quantity,
        "rt": req.validity,           "tp": req.trigger_price,
        "ts": req.trading_symbol,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.post(
                f"{sess['base_url']}/quick/order/rule/ms/modify",
                headers={
                    "Auth": sess["session_token"],
                    "Sid":  sess["session_sid"],
                    "neo-fin-key": NEO_FIN_KEY,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"jData": json.dumps(jData)},
            )
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# ORDER LIST
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/orders/list")
async def list_orders(req: LimitsRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(
                f"{sess['base_url']}/quick/user/orders",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY},
            )
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# POSITIONS
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/positions")
async def get_positions(req: LimitsRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(
                f"{sess['base_url']}/quick/user/positions",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY},
            )
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# TRADE BOOK
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/trades")
async def get_trades(req: LimitsRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(
                f"{sess['base_url']}/quick/user/trades",
                headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY},
            )
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# OPTION CHAIN (quotes for instrument)
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/chain/{instrument}")
async def get_chain(instrument: str, req: LimitsRequest):
    sess = get_session(req.session_id)
    inst = instrument.upper()
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            r = await client.get(
                f"{sess['base_url']}/script-details/1.0/quotes/neosymbol/nse_fo|{inst}/all",
                headers={"Authorization": sess["session_token"], "Content-Type": "application/json"},
            )
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────────────────────────────────
# SESSION INFO
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/session/{session_id}")
async def session_info(session_id: str):
    if session_id not in sessions:
        return {"valid": False}
    s = sessions[session_id]
    return {"valid": True, "created_at": s["created_at"], "base_url": s["base_url"]}

@app.delete("/session/{session_id}")
async def logout(session_id: str):
    if session_id in sessions:
        del sessions[session_id]
    return {"success": True, "message": "Logged out"}
