"""
api.py — FastAPI endpoint to serve OHLCV data from TimescaleDB.

This turns your database into a queryable API.
Other applications can fetch stock data via HTTP requests
instead of needing direct database access.
"""

import os
from datetime import date, timedelta
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

db_config = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "market_data"),
    "user": os.getenv("DB_USER", "market_pipeline"),
    "password": os.getenv("DB_PASSWORD"),
}


def get_db_connection():
    """
    Create a fresh database connection.
    
    We create a new connection for each request rather than sharing one.
    Why? Because if two people hit the API at the same time with a shared
    connection, their queries can interfere with each other. One connection
    per request is simpler and safer.
    
    RealDictCursor makes rows come back as dictionaries instead of tuples.
    Instead of row[0], row[1] you get row["date"], row["open"].
    This makes the code more readable and the JSON output automatic.
    """
    return psycopg2.connect(**db_config, cursor_factory=RealDictCursor)


# ── Create the FastAPI app ─────────────────────────────────────────
# This creates the application object. All routes are attached to it.
# In DocIQ you did the same thing.

app = FastAPI(
    title="Market Data API",
    description="Daily OHLCV data for US equities",
    version="1.0.0",
)


# ── Route 1: Health Check ──────────────────────────────────────────

@app.get("/")
def health_check():
    """
    A simple endpoint that confirms the API is running.
    
    Every production API has one of these. Monitoring tools
    (like AWS health checks) ping this URL regularly. If it
    stops responding, they send an alert.
    
    It also serves as a quick test: if you visit the URL in
    your browser and see this response, you know the server is up.
    """
    return {
        "status": "healthy",
        "service": "Market Data API",
        "version": "1.0.0",
    }


# ── Route 2: Get OHLCV Data ───────────────────────────────────────

@app.get("/api/ohlcv/{ticker}")
def get_ohlcv(
    ticker: str,
    start_date: date = Query(
        default=None,
        description="Start date (YYYY-MM-DD). Defaults to 30 days ago."
    ),
    end_date: date = Query(
        default=None,
        description="End date (YYYY-MM-DD). Defaults to today."
    ),
    limit: int = Query(
        default=100,
        ge=1,       # ge = "greater than or equal to" — minimum value is 1
        le=1000,    # le = "less than or equal to" — maximum value is 1000
        description="Max number of rows to return."
    ),
):
    """
    Fetch daily OHLCV data for a given stock ticker.
    
    This is the main endpoint. Examples:
      GET /api/ohlcv/AAPL
      GET /api/ohlcv/AAPL?start_date=2026-04-01&end_date=2026-04-18
      GET /api/ohlcv/AAPL?limit=5
    
    Query() is a FastAPI helper that:
    - Sets default values
    - Adds validation (ge, le)
    - Generates documentation automatically
    
    The 'ge' and 'le' constraints prevent abuse — without them,
    someone could request limit=999999999 and overload your database.
    """

    # Set defaults if no dates provided
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=30)

    # Convert ticker to uppercase so "aapl" and "AAPL" both work.
    # Users shouldn't have to worry about case sensitivity.
    ticker = ticker.upper()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        # First check: does this ticker exist in our database?
        # If not, return a clear error instead of an empty result.
        # HTTP 404 means "not found" — the standard code for this.
        cursor.execute(
            "SELECT symbol_id FROM symbols WHERE ticker = %s;",
            (ticker,)
        )
        symbol = cursor.fetchone()

        if not symbol:
            raise HTTPException(
                status_code=404,
                detail=f"Ticker '{ticker}' not found. "
                       f"We may not track this stock yet."
            )

        # Fetch the OHLCV data
        # ORDER BY time DESC returns newest data first.
        # LIMIT prevents returning millions of rows if someone
        # queries a huge date range.
        cursor.execute(
            """
            SELECT
                d.time::date AS date,
                d.open,
                d.high,
                d.low,
                d.close,
                d.volume,
                d.vwap,
                d.num_trades
            FROM daily_ohlcv d
            JOIN symbols s ON d.symbol_id = s.symbol_id
            WHERE s.ticker = %s
              AND d.time >= %s
              AND d.time <= %s
            ORDER BY d.time DESC
            LIMIT %s;
            """,
            (ticker, start_date, end_date, limit)
        )

        rows = cursor.fetchall()

        # Convert date objects to strings for JSON serialization.
        # JSON doesn't have a native date type, so we convert to
        # strings in ISO format (YYYY-MM-DD).
        for row in rows:
            row["date"] = row["date"].isoformat()
            # Convert Decimal to float for JSON.
            # PostgreSQL NUMERIC comes back as Python Decimal objects.
            # JSON doesn't understand Decimal, so we convert to float.
            for key in ["open", "high", "low", "close", "vwap"]:
                if row[key] is not None:
                    row[key] = float(row[key])

        return {
            "ticker": ticker,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "count": len(rows),
            "data": rows,
        }

    finally:
        conn.close()


# ── Route 3: List Available Tickers ────────────────────────────────

@app.get("/api/symbols")
def list_symbols():
    """
    Returns all tickers we have data for.
    
    This is a "discovery" endpoint — it tells users what data
    is available before they try to query specific tickers.
    Without this, a user would have to guess ticker names.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                s.ticker,
                s.name,
                s.sector,
                MIN(d.time)::date AS earliest_date,
                MAX(d.time)::date AS latest_date,
                COUNT(*) AS total_bars
            FROM symbols s
            JOIN daily_ohlcv d ON s.symbol_id = d.symbol_id
            WHERE s.is_active = TRUE
            GROUP BY s.ticker, s.name, s.sector
            ORDER BY s.ticker;
        """)

        rows = cursor.fetchall()

        for row in rows:
            if row["earliest_date"]:
                row["earliest_date"] = row["earliest_date"].isoformat()
            if row["latest_date"]:
                row["latest_date"] = row["latest_date"].isoformat()

        return {
            "count": len(rows),
            "symbols": rows,
        }

    finally:
        conn.close()


# ── Route 4: Pipeline Status ──────────────────────────────────────

@app.get("/api/pipeline/status")
def pipeline_status():
    """
    Shows the last 10 pipeline runs.
    
    This is your monitoring endpoint. You can check if the
    daily pipeline is running successfully, when it last ran,
    and if there were any errors. In a production environment,
    you'd connect a monitoring tool (like Grafana or Datadog)
    to this endpoint.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                run_id,
                run_type,
                status,
                started_at,
                finished_at,
                rows_fetched,
                rows_inserted,
                error_message
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 10;
        """)

        rows = cursor.fetchall()

        for row in rows:
            if row["started_at"]:
                row["started_at"] = row["started_at"].isoformat()
            if row["finished_at"]:
                row["finished_at"] = row["finished_at"].isoformat()

        return {
            "last_runs": rows,
        }

    finally:
        conn.close()