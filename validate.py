"""
validate.py — Data quality checks for OHLCV data.

In financial data, bad data = bad trades = lost money.
This script checks for the most common data quality issues:

1. Missing trading days (gaps in data)
2. Price anomalies (impossible values)
3. OHLC relationship violations
4. Volume anomalies
5. Stale data detection

These checks would run after every pipeline ingestion.
"""

import os
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import psycopg2

load_dotenv()

db_config = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "market_data"),
    "user": os.getenv("DB_USER", "market_pipeline"),
    "password": os.getenv("DB_PASSWORD"),
}


def check_missing_days(cursor, ticker, start_date, end_date):
    """
    Check 1: Are there any missing trading days?

    The US stock market is open Monday-Friday except holidays.
    If we're missing a weekday, either:
    - It was a holiday (acceptable)
    - Our pipeline failed to fetch it (problem!)

    We can't automatically know all holidays, but we can flag
    missing weekdays for manual review.
    """
    print("\n--- CHECK 1: Missing Trading Days ---")

    cursor.execute("""
        SELECT d.time::date AS trade_date
        FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s
          AND d.time >= %s
          AND d.time <= %s
        ORDER BY d.time;
    """, (ticker, start_date, end_date))

    trading_dates = {row[0] for row in cursor.fetchall()}

    # Generate all weekdays in the range
    current = start_date
    missing = []
    while current <= end_date:
        # weekday(): Monday=0, Tuesday=1, ..., Friday=4, Saturday=5, Sunday=6
        if current.weekday() < 5 and current not in trading_dates:
            missing.append(current)
        current += timedelta(days=1)

    if missing:
        print(f"  WARNING: {len(missing)} missing weekday(s):")
        for d in missing:
            day_name = d.strftime("%A")
            print(f"    - {d} ({day_name}) — could be a holiday or data gap")
    else:
        print("  PASS: No missing weekdays found.")

    return missing


def check_ohlc_relationships(cursor, ticker):
    """
    Check 2: Do the OHLC values make logical sense?

    For any trading day, these must ALWAYS be true:
    - High >= Open  (the high is the MAX price, must be >= everything)
    - High >= Close
    - High >= Low   (by definition)
    - Low <= Open   (the low is the MIN price, must be <= everything)
    - Low <= Close
    - All prices > 0

    If any of these fail, the data is corrupt.
    This catches API errors, bad splits adjustments, or data feed glitches.
    """
    print("\n--- CHECK 2: OHLC Relationship Violations ---")

    cursor.execute("""
        SELECT
            d.time::date,
            d.open, d.high, d.low, d.close
        FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s
          AND (
              d.high < d.open
              OR d.high < d.close
              OR d.high < d.low
              OR d.low > d.open
              OR d.low > d.close
              OR d.open <= 0
              OR d.high <= 0
              OR d.low <= 0
              OR d.close <= 0
          )
        ORDER BY d.time;
    """, (ticker,))

    violations = cursor.fetchall()

    if violations:
        print(f"  FAIL: {len(violations)} violation(s) found:")
        for v in violations:
            print(f"    - {v[0]}: O={v[1]} H={v[2]} L={v[3]} C={v[4]}")
    else:
        print("  PASS: All OHLC relationships are valid.")

    return violations


def check_price_anomalies(cursor, ticker):
    """
    Check 3: Are there any extreme price movements?

    A stock moving more than 20% in a single day is very unusual.
    It could mean:
    - A real crash/surge (rare but happens — e.g., earnings surprise)
    - A stock split that wasn't adjusted properly
    - Bad data from the API

    We flag these for human review. We don't auto-reject because
    sometimes 20%+ moves are real (e.g., GameStop in 2021).

    The formula: |(close - open) / open| > 0.20
    """
    print("\n--- CHECK 3: Extreme Price Movements (>20%) ---")

    cursor.execute("""
        SELECT
            d.time::date,
            d.open,
            d.close,
            ROUND(ABS((d.close - d.open) / NULLIF(d.open, 0)) * 100, 2)
                AS pct_change
        FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s
          AND ABS((d.close - d.open) / NULLIF(d.open, 0)) > 0.20
        ORDER BY d.time;
    """, (ticker,))

    anomalies = cursor.fetchall()

    if anomalies:
        print(f"  WARNING: {len(anomalies)} extreme move(s):")
        for a in anomalies:
            print(f"    - {a[0]}: Open=${a[1]} Close=${a[2]} "
                  f"Change={a[3]}%")
    else:
        print("  PASS: No extreme daily moves detected.")

    return anomalies


def check_volume_anomalies(cursor, ticker):
    """
    Check 4: Is volume suspiciously low or zero?

    Zero volume means no shares traded — this shouldn't happen for
    a stock like AAPL on a trading day. If volume is zero, the data
    is likely wrong.

    We also flag days where volume is less than 10% of the average.
    This could indicate a partial trading day (e.g., early close on
    holidays) or bad data.
    """
    print("\n--- CHECK 4: Volume Anomalies ---")

    # First check for zero volume
    cursor.execute("""
        SELECT d.time::date, d.volume
        FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s AND d.volume = 0
        ORDER BY d.time;
    """, (ticker,))

    zero_vol = cursor.fetchall()

    if zero_vol:
        print(f"  FAIL: {len(zero_vol)} day(s) with zero volume:")
        for z in zero_vol:
            print(f"    - {z[0]}")
    else:
        print("  PASS: No zero-volume days.")

    # Check for unusually low volume (< 10% of average)
    cursor.execute("""
        WITH avg_vol AS (
            SELECT AVG(d.volume) AS mean_vol
            FROM daily_ohlcv d
            JOIN symbols s ON d.symbol_id = s.symbol_id
            WHERE s.ticker = %s
        )
        SELECT d.time::date, d.volume, ROUND(av.mean_vol) AS avg_volume
        FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        CROSS JOIN avg_vol av
        WHERE s.ticker = %s
          AND d.volume < av.mean_vol * 0.10
        ORDER BY d.time;
    """, (ticker, ticker))

    low_vol = cursor.fetchall()

    if low_vol:
        print(f"  WARNING: {len(low_vol)} day(s) with unusually low volume:")
        for lv in low_vol:
            print(f"    - {lv[0]}: Volume={lv[1]:,} "
                  f"(avg={lv[2]:,})")
    else:
        print("  PASS: No unusually low volume days.")

    return zero_vol, low_vol


def check_stale_data(cursor, ticker):
    """
    Check 5: Is our data up to date?

    If the most recent data is more than 3 calendar days old,
    something might be wrong with our pipeline.

    Why 3 days? Because of weekends — if we check on Monday morning,
    the latest data might be from Friday (2 days ago). 3 gives us
    a buffer for holidays too.
    """
    print("\n--- CHECK 5: Data Freshness ---")

    cursor.execute("""
        SELECT MAX(d.time::date) AS latest_date
        FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s;
    """, (ticker,))

    latest = cursor.fetchone()[0]

    if latest:
        days_old = (date.today() - latest).days
        print(f"  Latest data: {latest} ({days_old} day(s) ago)")

        if days_old > 3:
            print(f"  WARNING: Data is {days_old} days old. "
                  f"Pipeline may have stopped.")
        else:
            print("  PASS: Data is fresh.")
    else:
        print("  FAIL: No data found for this ticker!")

    return latest


# ── Main execution ─────────────────────────────────────────────────
if __name__ == "__main__":
    ticker = "AAPL"

    print(f"Running data quality checks for {ticker}")
    print("=" * 60)

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Define date range for missing day check
    start_date = date(2026, 4, 8)
    end_date = date(2026, 4, 18)

    check_missing_days(cursor, ticker, start_date, end_date)
    check_ohlc_relationships(cursor, ticker)
    check_price_anomalies(cursor, ticker)
    check_volume_anomalies(cursor, ticker)
    check_stale_data(cursor, ticker)

    print("\n" + "=" * 60)
    print("Data quality checks complete.")

    conn.close()