"""
run_pipeline.py — Automated daily OHLCV pipeline.

This script:
1. Fetches daily OHLCV data from Massive.com
2. Inserts it into TimescaleDB
3. Runs data quality checks
4. Logs everything

It can run once (manually) or on a daily schedule.
"""

import os
import sys
from datetime import datetime, timedelta, date, timezone
from dotenv import load_dotenv
from massive import RESTClient
import psycopg2
from psycopg2.extras import execute_values, Json
from apscheduler.schedulers.blocking import BlockingScheduler

load_dotenv()

api_key = os.getenv("MASSIVE_API_KEY")
if not api_key:
    raise ValueError("MASSIVE_API_KEY not found in .env file!")

db_config = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "market_data"),
    "user": os.getenv("DB_USER", "market_pipeline"),
    "password": os.getenv("DB_PASSWORD"),
}

# ── Database Functions ─────────────────────────────────────────────

def ensure_symbol(cursor, ticker):
    """Insert ticker if it doesn't exist, return its symbol_id."""
    cursor.execute(
        """
        INSERT INTO symbols (ticker)
        VALUES (%s)
        ON CONFLICT (ticker) DO NOTHING
        RETURNING symbol_id;
        """,
        (ticker,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute(
            "SELECT symbol_id FROM symbols WHERE ticker = %s;",
            (ticker,)
        )
        return cursor.fetchone()[0]


def fetch_ohlcv(client, ticker, start_date, end_date):
    """Fetch daily OHLCV bars from Massive.com API."""
    bars = []
    for agg in client.list_aggs(
        ticker=ticker,
        multiplier=1,
        timespan="day",
        from_=start_date,
        to=end_date,
        adjusted=True,
        limit=50000,
    ):
        bars.append({
            "time": datetime.fromtimestamp(agg.timestamp / 1000, tz=timezone.utc),
            "open": agg.open,
            "high": agg.high,
            "low": agg.low,
            "close": agg.close,
            "volume": int(agg.volume),
            "vwap": agg.vwap,
            "num_trades": agg.transactions,
        })
    return bars


def insert_ohlcv(cursor, symbol_id, bars):
    """Bulk upsert OHLCV bars into TimescaleDB."""
    if not bars:
        return 0

    values = [
        (
            bar["time"], symbol_id,
            bar["open"], bar["high"], bar["low"], bar["close"],
            bar["volume"], bar["vwap"], bar["num_trades"], "massive",
        )
        for bar in bars
    ]

    query = """
        INSERT INTO daily_ohlcv
            (time, symbol_id, open, high, low, close, volume, vwap, num_trades, source)
        VALUES %s
        ON CONFLICT (symbol_id, time) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            vwap = EXCLUDED.vwap,
            num_trades = EXCLUDED.num_trades,
            source = EXCLUDED.source;
    """
    execute_values(cursor, query, values)
    return len(values)


def log_pipeline_run(cursor, run_type, status, rows_fetched,
                     rows_inserted, error_message=None, metadata=None):
    """Record this pipeline run for monitoring."""
    cursor.execute(
        """
        INSERT INTO pipeline_runs
            (run_type, status, finished_at, rows_fetched, rows_inserted,
             error_message, metadata)
        VALUES (%s, %s, NOW(), %s, %s, %s, %s)
        RETURNING run_id;
        """,
        (run_type, status, rows_fetched, rows_inserted,
         error_message, Json(metadata))
    )
    return cursor.fetchone()[0]

# ── Validation Functions ───────────────────────────────────────────

def validate_data(cursor, ticker, start_date, end_date):
    """
    Run all data quality checks. Returns a dict summarizing results.
    
    Instead of printing everything like validate.py did, this version
    collects results into a dictionary. Why? Because when this runs
    automatically at 10:30 PM, nobody is watching the terminal.
    We need results stored in a format we can log to the database
    or send as an alert later.
    """
    issues = []

    # Check 1: Missing trading days
    cursor.execute("""
        SELECT d.time::date FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s AND d.time >= %s AND d.time <= %s
        ORDER BY d.time;
    """, (ticker, start_date, end_date))

    trading_dates = {row[0] for row in cursor.fetchall()}

    current = start_date
    missing_days = []
    while current <= end_date:
        if current.weekday() < 5 and current not in trading_dates:
            missing_days.append(str(current))
        current += timedelta(days=1)

    if missing_days:
        issues.append(f"Missing {len(missing_days)} weekday(s): {missing_days}")

    # Check 2: OHLC relationship violations
    cursor.execute("""
        SELECT d.time::date, d.open, d.high, d.low, d.close
        FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s AND (
            d.high < d.open OR d.high < d.close OR d.high < d.low
            OR d.low > d.open OR d.low > d.close
            OR d.open <= 0 OR d.high <= 0 OR d.low <= 0 OR d.close <= 0
        );
    """, (ticker,))

    violations = cursor.fetchall()
    if violations:
        issues.append(f"OHLC violations on {len(violations)} day(s)")

    # Check 3: Extreme price movements (>20%)
    cursor.execute("""
        SELECT d.time::date,
            ROUND(ABS((d.close - d.open) / NULLIF(d.open, 0)) * 100, 2)
        FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s
          AND ABS((d.close - d.open) / NULLIF(d.open, 0)) > 0.20;
    """, (ticker,))

    extremes = cursor.fetchall()
    if extremes:
        issues.append(f"Extreme moves on {len(extremes)} day(s)")

    # Check 4: Zero volume
    cursor.execute("""
        SELECT d.time::date FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s AND d.volume = 0;
    """, (ticker,))

    zero_vol = cursor.fetchall()
    if zero_vol:
        issues.append(f"Zero volume on {len(zero_vol)} day(s)")

    return {
        "checks_passed": len(issues) == 0,
        "issues": issues,
        "missing_days": missing_days,
    }

# ── Main Pipeline ──────────────────────────────────────────────────

def run_daily_pipeline():
    """
    The main pipeline function. Called once per day.
    
    This is what the scheduler triggers. It's wrapped in a function
    (not just loose code) because APScheduler needs to call it.
    
    The structure is:
    1. Figure out what date range to fetch
    2. Fetch and insert data
    3. Validate the data
    4. Log everything
    """
    ticker = "AAPL"

    # We fetch the last 5 trading days every time, not just "today."
    # Why? Because:
    # - The API might retroactively correct yesterday's data
    # - If the pipeline failed yesterday, today's run fills the gap
    # - It's idempotent so duplicates aren't a problem
    # This pattern is called "overlapping windows" and is common
    # in production pipelines.
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"Pipeline run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Ticker: {ticker} | Range: {start_date} to {end_date}")
    print(f"{'='*60}")

    client = RESTClient(api_key=api_key)
    conn = psycopg2.connect(**db_config)

    try:
        conn.autocommit = False
        cursor = conn.cursor()

        # Step 1: Ensure symbol exists
        symbol_id = ensure_symbol(cursor, ticker)
        print(f"Symbol: {ticker} (id={symbol_id})")

        # Step 2: Fetch from API
        print("Fetching from Massive.com...")
        bars = fetch_ohlcv(client, ticker, start_date, end_date)
        print(f"Fetched {len(bars)} bars")

        # Step 3: Insert into TimescaleDB
        print("Inserting into TimescaleDB...")
        rows_inserted = insert_ohlcv(cursor, symbol_id, bars)
        print(f"Inserted/updated {rows_inserted} rows")

        # Step 4: Validate
        print("Running validation...")
        start_dt = datetime.now().date() - timedelta(days=7)
        end_dt = datetime.now().date()
        validation = validate_data(cursor, ticker, start_dt, end_dt)

        if validation["checks_passed"]:
            print("Validation: ALL CHECKS PASSED")
        else:
            print(f"Validation: {len(validation['issues'])} issue(s):")
            for issue in validation["issues"]:
                print(f"  - {issue}")

        # Step 5: Log the run
        run_id = log_pipeline_run(
            cursor,
            run_type="daily_ohlcv",
            status="success",
            rows_fetched=len(bars),
            rows_inserted=rows_inserted,
            metadata={
                "ticker": ticker,
                "start": start_date,
                "end": end_date,
                "validation": validation,
            }
        )

        conn.commit()
        print(f"SUCCESS — run_id={run_id}")

    except Exception as e:
        conn.rollback()
        print(f"FAILED: {e}")

        try:
            conn.autocommit = True
            cursor = conn.cursor()
            log_pipeline_run(
                cursor,
                run_type="daily_ohlcv",
                status="failed",
                rows_fetched=0,
                rows_inserted=0,
                error_message=str(e),
                metadata={"ticker": ticker}
            )
        except Exception:
            print("Could not log failure to database.")

        raise

    finally:
        conn.close()

# ── Entry Point ────────────────────────────────────────────────────

if __name__ == "__main__":
    # sys.argv is a list of command-line arguments.
    # sys.argv[0] is always the script name ("run_pipeline.py")
    # sys.argv[1] would be the first argument you type after the script name.
    #
    # This lets us run the script two ways:
    #   python run_pipeline.py          → runs once immediately
    #   python run_pipeline.py schedule → starts the daily scheduler
    #
    # Why both modes? "Run once" is for testing and manual runs.
    # "Schedule" is for production — it runs and then waits until
    # tomorrow to run again.

    if len(sys.argv) > 1 and sys.argv[1] == "schedule":
        print("Starting daily pipeline scheduler...")
        print("Pipeline will run daily at 22:30 UTC (6:30 PM ET)")
        print("Press Ctrl+C to stop.\n")

        # BlockingScheduler means "start scheduling and don't do
        # anything else." The script will sit here waiting for the
        # next scheduled time. "Blocking" because it blocks the
        # program from continuing — it takes over.
        #
        # The alternative is BackgroundScheduler which runs in the
        # background while your program does other things. We don't
        # need that here because this script's only job IS scheduling.

        scheduler = BlockingScheduler()

        # add_job tells the scheduler WHAT to run and WHEN.
        #   func: the function to call (our pipeline)
        #   trigger: "cron" means calendar-based scheduling
        #   hour=22, minute=30: run at 22:30 UTC every day
        #
        # "cron" comes from Unix cron jobs — one of the oldest
        # scheduling systems. The name is from Greek "chronos" (time).
        # APScheduler uses the same concept but inside Python.

        scheduler.add_job(
            func=run_daily_pipeline,
            trigger="cron",
            hour=22,
            minute=30,
            id="daily_ohlcv",
            name="Daily OHLCV Pipeline",
            misfire_grace_time=3600,  # If we miss the time by up to
                                      # 1 hour (3600 seconds), still run.
                                      # Handles cases where the server
                                      # was briefly down at 22:30.
        )

        # Run once immediately on startup so we don't have to wait
        # until 22:30 to know if it works.
        print("Running initial pipeline...")
        run_daily_pipeline()

        print("\nScheduler active. Waiting for next run at 22:30 UTC...")
        scheduler.start()  # This line blocks forever (until Ctrl+C)

    else:
        # No "schedule" argument — just run once and exit
        run_daily_pipeline()

