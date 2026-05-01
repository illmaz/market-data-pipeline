import os
from dotenv import load_dotenv
from fastapi import FastAPI, Security, HTTPException
from fastapi.security import APIKeyHeader
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

load_dotenv()
from fastapi.templating import Jinja2Templates
templates = Jinja2Templates(directory="templates")

# ── Rate limiter ───────────────────────────────────────────────────
# Tracks requests per IP address. Limits are applied per route.
limiter = Limiter(key_func=get_remote_address)

# ── API Key verification ───────────────────────────────────────────
# Reads the API_KEY from .env and checks every protected request.
# Protected routes must include Depends(verify_api_key).
# Public routes (dashboard HTML, dashboard API) do not.

API_KEY = os.getenv("API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(key: str = Security(api_key_header)):
    if not key or key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. Include X-API-Key header."
        )

# ── Database connection pool ───────────────────────────────────────
db_config = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "6432")),
    "dbname": os.getenv("DB_NAME", "market_data"),
    "user": os.getenv("DB_USER", "market_pipeline"),
    "password": os.getenv("DB_PASSWORD"),
}

pool = ThreadedConnectionPool(
    minconn=2,
    maxconn=10,
    cursor_factory=RealDictCursor,
    **db_config
)

def get_db_connection():
    return pool.getconn()

# ── Single FastAPI app ─────────────────────────────────────────────
app = FastAPI(
    title="Market Data API",
    description="Daily OHLCV data for US equities",
    version="2.0.0",
)

# Attach rate limiter to the app
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── Register routes ────────────────────────────────────────────────
from api import router as api_router
from dashboard import router as dashboard_router

app.include_router(api_router)
app.include_router(dashboard_router)