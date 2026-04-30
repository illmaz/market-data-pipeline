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
import time
from datetime import datetime, timedelta, date, timezone
from dotenv import load_dotenv
from massive import RESTClient
import psycopg2
from utils import ensure_symbol, fetch_ohlcv, insert_ohlcv, log_pipeline_run, fetch_grouped_daily


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
    Daily pipeline using Grouped Daily endpoint.
    One API call fetches all stocks. Then loop to insert each.
    """
    # Fetch last 3 trading days to cover weekends and gaps
    # We check each date separately with the grouped endpoint
    dates_to_fetch = []
    for days_back in range(1, 5):  # yesterday through 4 days ago
        d = (datetime.now(tz=timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
        dates_to_fetch.append(d)

    print(f"\n{'='*60}")
    print(f"Pipeline run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Dates to fetch: {dates_to_fetch}")
    print(f"{'='*60}")

    client = RESTClient(api_key=api_key)
    conn = psycopg2.connect(**db_config)

    try:
        conn.autocommit = False
        cursor = conn.cursor()

        total_inserted = 0
        total_fetched = 0

        for date_str in dates_to_fetch:
            print(f"\nFetching grouped daily for {date_str}...")
            try:
                grouped = fetch_grouped_daily(client, date_str)
            except Exception as e:
                print(f"  Failed to fetch {date_str}: {e}")
                continue

            if not grouped:
                print(f"  No data for {date_str} (weekend/holiday)")
                continue

            # Filter to only stocks in our symbols table
            cursor.execute("SELECT ticker, symbol_id FROM symbols WHERE is_active = TRUE;")
            symbol_map = {row[0]: row[1] for row in cursor.fetchall()}

            date_inserted = 0
            for ticker, bar in grouped.items():
                if ticker not in symbol_map:
                    continue  # skip stocks we don't track

                symbol_id = symbol_map[ticker]
                rows = insert_ohlcv(cursor, symbol_id, [bar])
                date_inserted += rows

            total_fetched += len(grouped)
            total_inserted += date_inserted
            print(f"  Got {len(grouped)} stocks, inserted {date_inserted} for tracked symbols")

            # Rate limit: pause between dates
            time.sleep(13)  # 5 calls per minute = 1 call per 12 seconds

        # Validate
        print("\nRunning validation...")
        start_dt = date.fromisoformat(dates_to_fetch[-1])
        end_dt = date.fromisoformat(dates_to_fetch[0])
        validation = validate_data(cursor, "SPY", start_dt, end_dt)

        if validation["checks_passed"]:
            print("Validation: ALL CHECKS PASSED")
        else:
            for issue in validation["issues"]:
                print(f"  - {issue}")

        # Log the run
        run_id = log_pipeline_run(
            cursor,
            run_type="daily_ohlcv_grouped",
            status="success",
            rows_fetched=total_fetched,
            rows_inserted=total_inserted,
            metadata={
                "dates": dates_to_fetch,
                "validation": validation,
            }
        )

        conn.commit()
        print(f"\nSUCCESS — run_id={run_id}")
        print(f"Total: {total_fetched} fetched, {total_inserted} inserted")

    except Exception as e:
        conn.rollback()
        print(f"FAILED: {e}")

        try:
            conn.autocommit = True
            cursor = conn.cursor()
            log_pipeline_run(
                cursor,
                run_type="daily_ohlcv_grouped",
                status="failed",
                rows_fetched=0,
                rows_inserted=0,
                error_message=str(e),
            )
        except Exception:
            print("Could not log failure to database.")
        raise

    finally:
        conn.close()

# ── Entry Point ────────────────────────────────────────────────────

if __name__ == "__main__":
    run_daily_pipeline()
