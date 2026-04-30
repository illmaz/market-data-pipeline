"""
dashboard.py — Market Data Pipeline Dashboard API.

Adds dashboard endpoints to the FastAPI app that show:
- Pipeline health and run history
- Data coverage statistics
- Data quality overview
- Stock browser with basic stats
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
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Market Data Pipeline Dashboard</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                             Roboto, sans-serif;
                background: #0f1117;
                color: #e1e4e8;
                padding: 20px;
            }
            h1 { color: #58a6ff; margin-bottom: 20px; }
            h2 { color: #8b949e; margin: 20px 0 10px; font-size: 14px;
                 text-transform: uppercase; letter-spacing: 1px; }
            .grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 16px;
                margin-bottom: 24px;
            }
            .card {
                background: #161b22;
                border: 1px solid #30363d;
                border-radius: 8px;
                padding: 20px;
            }
            .card .label { color: #8b949e; font-size: 13px; }
            .card .value {
                font-size: 32px; font-weight: bold;
                color: #58a6ff; margin-top: 4px;
            }
            .card .sub { color: #8b949e; font-size: 12px; margin-top: 4px; }
            .status-good { color: #3fb950; }
            .status-warning { color: #d29922; }
            .status-failed { color: #f85149; }
            table {
                width: 100%; border-collapse: collapse;
                background: #161b22; border-radius: 8px;
                overflow: hidden;
            }
            th {
                background: #21262d; color: #8b949e;
                padding: 10px 16px; text-align: left;
                font-size: 12px; text-transform: uppercase;
            }
            td { padding: 10px 16px; border-top: 1px solid #30363d; }
            .positive { color: #3fb950; }
            .negative { color: #f85149; }
            #loading { color: #8b949e; padding: 40px; text-align: center; }
        </style>
    </head>
    <body>
        <h1>📈 Market Data Pipeline</h1>
        <div id="loading">Loading dashboard data...</div>
        <div id="content" style="display:none;">

            <h2>Pipeline Overview</h2>
            <div class="grid" id="overview-cards"></div>

            <h2>Top 10 Gainers (Latest Trading Day)</h2>
            <table id="gainers-table"><thead><tr>
                <th>Ticker</th><th>Name</th><th>Sector</th>
                <th>Open</th><th>Close</th><th>Change %</th>
            </tr></thead><tbody></tbody></table>

            <h2>Top 10 Losers</h2>
            <table id="losers-table"><thead><tr>
                <th>Ticker</th><th>Name</th><th>Sector</th>
                <th>Open</th><th>Close</th><th>Change %</th>
            </tr></thead><tbody></tbody></table>

            <h2>Sector Coverage</h2>
            <table id="sectors-table"><thead><tr>
                <th>Sector</th><th>Stocks</th><th>Total Bars</th>
            </tr></thead><tbody></tbody></table>

            <h2>Data Quality</h2>
            <div class="grid" id="quality-cards"></div>

        </div>

        <script>
            async function loadDashboard() {
                try {
                    const [overview, movers, sectors, quality] =
                        await Promise.all([
                            fetch('/api/dashboard/overview').then(r => r.json()),
                            fetch('/api/dashboard/movers').then(r => r.json()),
                            fetch('/api/dashboard/sectors').then(r => r.json()),
                            fetch('/api/dashboard/data-quality').then(r => r.json()),
                        ]);

                    const statusClass = overview.last_pipeline_run?.status === 'success'
                        ? 'status-good' : 'status-failed';

                    document.getElementById('overview-cards').innerHTML = `
                        <div class="card">
                            <div class="label">Stocks Tracked</div>
                            <div class="value">${overview.stocks_tracked}</div>
                        </div>
                        <div class="card">
                            <div class="label">Total Data Points</div>
                            <div class="value">${overview.total_data_points?.toLocaleString()}</div>
                        </div>
                        <div class="card">
                            <div class="label">Date Range</div>
                            <div class="value" style="font-size:18px">
                                ${overview.date_range?.earliest_date || 'N/A'} →
                                ${overview.date_range?.latest_date || 'N/A'}
                            </div>
                        </div>
                        <div class="card">
                            <div class="label">Last Pipeline Run</div>
                            <div class="value ${statusClass}" style="font-size:24px">
                                ${overview.last_pipeline_run?.status?.toUpperCase() || 'N/A'}
                            </div>
                            <div class="sub">
                                ${overview.pipeline_reliability?.successes}/${overview.pipeline_reliability?.last_30_runs} successful
                            </div>
                        </div>
                    `;

                    function renderMovers(tableId, stocks) {
                        const tbody = document.querySelector(`#${tableId} tbody`);
                        tbody.innerHTML = stocks?.map(s => `
                            <tr>
                                <td><strong>${s.ticker}</strong></td>
                                <td>${s.name || ''}</td>
                                <td>${s.sector || ''}</td>
                                <td>$${s.open?.toFixed(2)}</td>
                                <td>$${s.close?.toFixed(2)}</td>
                                <td class="${s.daily_return_pct >= 0 ? 'positive' : 'negative'}">
                                    ${s.daily_return_pct >= 0 ? '+' : ''}${s.daily_return_pct}%
                                </td>
                            </tr>
                        `).join('') || '<tr><td colspan="6">No data</td></tr>';
                    }
                    renderMovers('gainers-table', movers.top_10_gainers);
                    renderMovers('losers-table', movers.top_10_losers);

                    const sectorsTbody = document.querySelector('#sectors-table tbody');
                    sectorsTbody.innerHTML = sectors.sectors?.map(s => `
                        <tr>
                            <td>${s.sector}</td>
                            <td>${s.stock_count}</td>
                            <td>${s.total_bars?.toLocaleString()}</td>
                        </tr>
                    `).join('') || '';

                    const qClass = quality.health_score === 'GOOD'
                        ? 'status-good'
                        : quality.health_score === 'WARNING'
                            ? 'status-warning' : 'status-failed';

                    document.getElementById('quality-cards').innerHTML = `
                        <div class="card">
                            <div class="label">Health Score</div>
                            <div class="value ${qClass}">${quality.health_score}</div>
                        </div>
                        <div class="card">
                            <div class="label">OHLC Violations</div>
                            <div class="value">${quality.ohlc_violations}</div>
                        </div>
                        <div class="card">
                            <div class="label">Zero Volume Days (30d)</div>
                            <div class="value">${quality.zero_volume_days_last_30d}</div>
                        </div>
                        <div class="card">
                            <div class="label">Low Coverage Stocks</div>
                            <div class="value">${quality.low_coverage_stocks?.length || 0}</div>
                        </div>
                    `;

                    document.getElementById('loading').style.display = 'none';
                    document.getElementById('content').style.display = 'block';

                } catch (e) {
                    document.getElementById('loading').innerText =
                        'Error loading dashboard: ' + e.message;
                }
            }

            loadDashboard();
        </script>
    </body>
    </html>
    """