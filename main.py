"""
NEXUS Trading Bot — FastAPI Backend + Frontend Server
Opens directly in Chrome on your phone at your Railway URL
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import httpx
import json
from datetime import datetime
from pydantic import BaseModel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NEXUS Trading Bot", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

KOTAK_LOGIN_URL    = "https://mis.kotaksecurities.com/login/1.0/tradeApiLogin"
KOTAK_VALIDATE_URL = "https://mis.kotaksecurities.com/login/1.0/tradeApiValidate"
NEO_FIN_KEY        = "neotradeapi"
sessions: dict     = {}

class LoginRequest(BaseModel):
    access_token: str; mobile: str; ucc: str; totp: str

class ValidateRequest(BaseModel):
    access_token: str; view_token: str; view_sid: str; mpin: str

class SessionRequest(BaseModel):
    session_id: str

class OrderRequest(BaseModel):
    session_id: str; trading_symbol: str; transaction_type: str; quantity: str
    order_type: str = "MKT"; price: str = "0"; product: str = "NRML"
    validity: str = "DAY"; exchange_segment: str = "nse_fo"
    amo: str = "NO"; disclosed_quantity: str = "0"
    market_protection: str = "0"; pf: str = "N"; trigger_price: str = "0"

class CancelRequest(BaseModel):
    session_id: str; order_no: str; am: str = "NO"

def get_session(sid):
    if sid not in sessions:
        raise HTTPException(status_code=401, detail="Session expired. Login again.")
    return sessions[sid]

BOT_HTML = open("bot.html").read() if __import__("os").path.exists("bot.html") else "<h1>bot.html not found</h1>"

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    return HTMLResponse(content=BOT_HTML)

@app.get("/health")
async def health():
    return {"status": "NEXUS Backend Running", "version": "2.0.0", "time": datetime.now().isoformat()}

@app.post("/auth/login")
async def login(req: LoginRequest):
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(KOTAK_LOGIN_URL, headers={"Authorization": req.access_token, "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/json"}, json={"mobileNumber": req.mobile, "ucc": req.ucc, "totp": req.totp})
            d = r.json()
            if d.get("data", {}).get("token"):
                return {"success": True, "view_token": d["data"]["token"], "view_sid": d["data"]["sid"]}
            return {"success": False, "message": d.get("message", "Login failed")}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/auth/validate")
async def validate(req: ValidateRequest):
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(KOTAK_VALIDATE_URL, headers={"Authorization": req.access_token, "neo-fin-key": NEO_FIN_KEY, "sid": req.view_sid, "Auth": req.view_token, "Content-Type": "application/json"}, json={"mpin": req.mpin})
            d = r.json()
            if d.get("data", {}).get("token"):
                sid = f"sess_{req.view_sid[-8:]}"
                sessions[sid] = {"session_token": d["data"]["token"], "session_sid": d["data"]["sid"], "base_url": d["data"].get("baseUrl","https://cis.kotaksecurities.com"), "created_at": datetime.now().isoformat()}
                return {"success": True, "session_id": sid}
            return {"success": False, "message": d.get("message", "MPIN failed")}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/wallet/limits")
async def limits(req: SessionRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(f"{sess['base_url']}/quick/user/limits", headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/x-www-form-urlencoded"}, data={"jData": json.dumps({"seg":"ALL","exch":"ALL","prod":"ALL"})})
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/positions")
async def positions(req: SessionRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.get(f"{sess['base_url']}/quick/user/positions", headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY})
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/list")
async def orders_list(req: SessionRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.get(f"{sess['base_url']}/quick/user/orders", headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY})
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/place")
async def place_order(req: OrderRequest):
    sess = get_session(req.session_id)
    jData = {"am":req.amo,"dq":req.disclosed_quantity,"es":req.exchange_segment,"mp":req.market_protection,"pc":req.product,"pf":req.pf,"pr":req.price,"pt":req.order_type,"qt":req.quantity,"rt":req.validity,"tp":req.trigger_price,"ts":req.trading_symbol,"tt":req.transaction_type}
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(f"{sess['base_url']}/quick/order/rule/ms/place", headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/x-www-form-urlencoded"}, data={"jData": json.dumps(jData)})
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/cancel")
async def cancel_order(req: CancelRequest):
    sess = get_session(req.session_id)
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.post(f"{sess['base_url']}/quick/order/cancel", headers={"Auth": sess["session_token"], "Sid": sess["session_sid"], "neo-fin-key": NEO_FIN_KEY, "Content-Type": "application/x-www-form-urlencoded"}, data={"jData": json.dumps({"am":req.am,"on":req.order_no})})
            return r.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
