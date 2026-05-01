"""
dashboard.py — Market Data Pipeline Dashboard API.

Adds dashboard endpoints to the FastAPI app that show:
- Pipeline health and run history
- Data coverage statistics
- Data quality overview
- Interactive price chart with MA20/MA50
- Clickable movers tables
- Sector filter
"""
from datetime import date
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from main import get_db_connection, pool, limiter

router = APIRouter()


# ── Dashboard: Overview Stats ──────────────────────────────────────

@router.get("/api/dashboard/overview")
@limiter.limit("60/minute")
def dashboard_overview(request: Request):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(DISTINCT symbol_id) AS stock_count
            FROM daily_ohlcv;
        """)
        stock_count = cursor.fetchone()["stock_count"]

        cursor.execute("SELECT COUNT(*) AS total_rows FROM daily_ohlcv;")
        total_rows = cursor.fetchone()["total_rows"]

        cursor.execute("""
            SELECT
                MIN(time)::date AS earliest_date,
                MAX(time)::date AS latest_date
            FROM daily_ohlcv;
        """)
        date_range = cursor.fetchone()

        cursor.execute("""
            SELECT run_type, status, started_at, finished_at,
                   rows_fetched, rows_inserted, error_message
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 1;
        """)
        last_run = cursor.fetchone()

        cursor.execute("""
            SELECT
                COUNT(*) AS total_runs,
                COUNT(*) FILTER (WHERE status = 'success') AS success_count,
                COUNT(*) FILTER (WHERE status = 'failed') AS fail_count
            FROM (
                SELECT status FROM pipeline_runs
                ORDER BY started_at DESC LIMIT 30
            ) recent;
        """)
        run_stats = cursor.fetchone()

        if last_run and last_run["started_at"]:
            last_run["started_at"] = last_run["started_at"].isoformat()
        if last_run and last_run["finished_at"]:
            last_run["finished_at"] = last_run["finished_at"].isoformat()
        if date_range["earliest_date"]:
            date_range["earliest_date"] = date_range["earliest_date"].isoformat()
        if date_range["latest_date"]:
            date_range["latest_date"] = date_range["latest_date"].isoformat()

        return {
            "stocks_tracked": stock_count,
            "total_data_points": total_rows,
            "date_range": date_range,
            "last_pipeline_run": last_run,
            "pipeline_reliability": {
                "last_30_runs": run_stats["total_runs"],
                "successes": run_stats["success_count"],
                "failures": run_stats["fail_count"],
            },
        }

    finally:
        pool.putconn(conn)


# ── Dashboard: Sector Breakdown ────────────────────────────────────

@router.get("/api/dashboard/sectors")
@limiter.limit("60/minute")
def sector_breakdown(request: Request):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COALESCE(s.sector, 'Unknown') AS sector,
                COUNT(DISTINCT s.symbol_id) AS stock_count,
                SUM(bar_counts.bars) AS total_bars
            FROM symbols s
            JOIN (
                SELECT symbol_id, COUNT(*) AS bars
                FROM daily_ohlcv
                GROUP BY symbol_id
            ) bar_counts ON s.symbol_id = bar_counts.symbol_id
            GROUP BY s.sector
            ORDER BY stock_count DESC;
        """)
        rows = cursor.fetchall()
        return {
            "sector_count": len(rows),
            "sectors": rows,
        }

    finally:
        pool.putconn(conn)


# ── Dashboard: Top Movers ──────────────────────────────────────────

@router.get("/api/dashboard/movers")
@limiter.limit("60/minute")
def top_movers(
    request: Request,
    trading_date: date = Query(
        default=None,
        description="Date to check (defaults to latest available)"
    ),
):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        if trading_date is None:
            cursor.execute("SELECT MAX(time)::date FROM daily_ohlcv;")
            trading_date = cursor.fetchone()["max"]

        if trading_date is None:
            return {"error": "No data available"}

        cursor.execute("""
            SELECT
                s.ticker,
                s.name,
                s.sector,
                d.open,
                d.close,
                d.volume,
                ROUND(((d.close - d.open) / NULLIF(d.open, 0)) * 100, 2)
                    AS daily_return_pct
            FROM daily_ohlcv d
            JOIN symbols s ON d.symbol_id = s.symbol_id
            WHERE d.time::date = %s
              AND d.open > 0
            ORDER BY daily_return_pct DESC;
        """, (trading_date,))

        all_stocks = cursor.fetchall()

        for stock in all_stocks:
            for key in ["open", "close", "daily_return_pct"]:
                if stock[key] is not None:
                    stock[key] = float(stock[key])

        gainers = all_stocks[:10]
        losers = list(reversed(all_stocks[-10:])) if len(all_stocks) >= 10 else []

        return {
            "trading_date": trading_date.isoformat(),
            "total_stocks": len(all_stocks),
            "top_10_gainers": gainers,
            "top_10_losers": losers,
        }

    finally:
        pool.putconn(conn)


# ── Dashboard: Data Quality Report ─────────────────────────────────

@router.get("/api/dashboard/data-quality")
@limiter.limit("60/minute")
def data_quality_report(request: Request):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT s.ticker, s.name, COUNT(*) AS bar_count
            FROM daily_ohlcv d
            JOIN symbols s ON d.symbol_id = s.symbol_id
            GROUP BY s.ticker, s.name
            HAVING COUNT(*) < 400
            ORDER BY bar_count ASC
            LIMIT 20;
        """)
        low_coverage = cursor.fetchall()

        cursor.execute("""
            SELECT s.ticker, d.time::date AS trade_date, d.volume
            FROM daily_ohlcv d
            JOIN symbols s ON d.symbol_id = s.symbol_id
            WHERE d.volume = 0
              AND d.time >= NOW() - INTERVAL '30 days'
            ORDER BY d.time DESC
            LIMIT 20;
        """)
        zero_volume = cursor.fetchall()
        for row in zero_volume:
            row["trade_date"] = row["trade_date"].isoformat()

        cursor.execute("""
            SELECT COUNT(*) AS violation_count
            FROM daily_ohlcv
            WHERE high < low
               OR high < open
               OR high < close
               OR low > open
               OR low > close
               OR open <= 0;
        """)
        violations = cursor.fetchone()["violation_count"]

        cursor.execute(
            "SELECT COUNT(DISTINCT symbol_id) FROM daily_ohlcv;"
        )
        total_stocks = cursor.fetchone()["count"]

        cursor.execute("SELECT COUNT(*) FROM daily_ohlcv;")
        total_rows = cursor.fetchone()["count"]

        health_score = "GOOD"
        if violations > 0:
            health_score = "WARNING"
        if len(low_coverage) > total_stocks * 0.1:
            health_score = "POOR"

        return {
            "health_score": health_score,
            "total_stocks": total_stocks,
            "total_rows": total_rows,
            "ohlc_violations": violations,
            "zero_volume_days_last_30d": len(zero_volume),
            "zero_volume_details": zero_volume,
            "low_coverage_stocks": low_coverage,
        }

    finally:
        pool.putconn(conn)


# ── Dashboard: HTML Page ───────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
@limiter.limit("60/minute")
def dashboard_page(request: Request):
    # Pull the API key from the app's environment so the frontend
    # can call the protected /api/ohlcv/{ticker} endpoint.
    # This is acceptable for a private internal dashboard.
    from main import templates
    import os
    api_key = os.getenv("API_KEY", "")
    return templates.TemplateResponse(request, "dashboard.html", {
        "api_key": api_key,
    })
