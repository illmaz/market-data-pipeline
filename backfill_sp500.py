"""
backfill_sp500.py — Backfill 2 years of daily OHLCV for all S&P 500 stocks.

This script:
1. Downloads the current S&P 500 ticker list
2. Fetches 2 years of history for each ticker
3. Stores in TimescaleDB
4. Saves as Parquet on S3

Rate limit: 5 API calls/minute, so this takes ~100 minutes.
Run it once, then daily updates use the Grouped Daily endpoint (1 call).
"""

import os
import time
import io
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from massive import RESTClient
import psycopg2
from psycopg2.extras import execute_values, Json
import pandas as pd
import boto3

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

S3_BUCKET = "market-data-pipeline-illmaz"


# ── Get S&P 500 tickers ───────────────────────────────────────────

def get_sp500_tickers():
    """
    Download the current S&P 500 ticker list from GitHub.

    This CSV is updated daily and sourced from Wikipedia's S&P 500 page.
    We use it instead of hardcoding because companies get added and
    removed from the index regularly (about 20-30 changes per year).
    """
    url = ("https://raw.githubusercontent.com/datasets/"
           "s-and-p-500-companies/main/data/constituents.csv")

    print("Downloading S&P 500 ticker list...")
    df = pd.read_csv(url)

    # The CSV has columns: Symbol, Name, Sector
    # Some tickers have dots (e.g., BRK.B) which the API expects as
    # a hyphen or just the base ticker. Let's keep them as-is for now.
    tickers = sorted(df["Symbol"].tolist())

    print(f"Found {len(tickers)} tickers")
    return tickers, df


# ── Reused functions ───────────────────────────────────────────────

def ensure_symbol(cursor, ticker, name=None, sector=None):
    """Insert ticker with name and sector if it doesn't exist."""
    cursor.execute(
        """
        INSERT INTO symbols (ticker, name, sector)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, symbols.name),
            sector = COALESCE(EXCLUDED.sector, symbols.sector),
            updated_at = NOW()
        RETURNING symbol_id;
        """,
        (ticker, name, sector)
    )
    return cursor.fetchone()[0]


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
            open = EXCLUDED.open, high = EXCLUDED.high,
            low = EXCLUDED.low, close = EXCLUDED.close,
            volume = EXCLUDED.volume, vwap = EXCLUDED.vwap,
            num_trades = EXCLUDED.num_trades, source = EXCLUDED.source;
    """
    execute_values(cursor, query, values)
    return len(values)


def fetch_historical(client, ticker, start_date, end_date):
    """Fetch all daily OHLCV bars for a ticker."""
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
            "time": datetime.fromtimestamp(
                agg.timestamp / 1000, tz=timezone.utc
            ),
            "ticker": ticker,
            "open": agg.open,
            "high": agg.high,
            "low": agg.low,
            "close": agg.close,
            "volume": int(agg.volume),
            "vwap": agg.vwap,
            "num_trades": agg.transactions,
        })
    return bars


def save_to_parquet_s3(bars, ticker):
    """Save bars as Parquet and upload to S3."""
    if not bars:
        return None

    df = pd.DataFrame(bars)
    df["time"] = pd.to_datetime(df["time"])

    local_path = f"/tmp/{ticker}_daily_ohlcv.parquet"
    df.to_parquet(local_path, engine="pyarrow", index=False)

    s3_client = boto3.client("s3")
    s3_key = f"daily_ohlcv/ticker={ticker}/data.parquet"
    s3_client.upload_file(local_path, S3_BUCKET, s3_key)

    os.remove(local_path)
    return s3_key


# ── Main backfill logic ───────────────────────────────────────────

if __name__ == "__main__":
    # Date range: 2 years
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")

    print("S&P 500 Historical Backfill")
    print(f"Range: {start_date} to {end_date}")
    print("=" * 60)

    # Step 1: Get ticker list
    tickers, sp500_df = get_sp500_tickers()

    # Build a lookup for name and sector
    ticker_info = {}
    for _, row in sp500_df.iterrows():
        ticker_info[row["Symbol"]] = {
            "name": row.get("Name", None),
            "sector": row.get("Sector", None),
        }

    # Step 2: Set up API client and database
    client = RESTClient(api_key=api_key)
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True  # Commit after each ticker so we don't
                             # lose everything if it crashes at ticker #400
    cursor = conn.cursor()

    # Track progress
    total = len(tickers)
    success = 0
    failed = []
    api_calls = 0
    start_time = time.time()

    print(f"\nBackfilling {total} tickers...")
    print(f"Estimated time: ~{total // 5} minutes")
    print(f"Started at: {datetime.now().strftime('%H:%M:%S')}\n")

    for i, ticker in enumerate(tickers):
        try:
            # Progress indicator
            elapsed = time.time() - start_time
            if i > 0:
                per_ticker = elapsed / i
                remaining = per_ticker * (total - i)
                eta = f"~{int(remaining // 60)}m {int(remaining % 60)}s"
            else:
                eta = "calculating..."

            print(f"[{i+1}/{total}] {ticker} (ETA: {eta})")

            # Rate limiting: 5 calls per minute
            # After every 5 calls, pause until 60 seconds have passed
            api_calls += 1
            if api_calls % 5 == 0 and api_calls > 0:
                # Calculate how long we've been going since last pause
                pause_time = max(0, 61 - (time.time() - start_time) % 61)
                if pause_time > 1:
                    print(f"  Rate limit pause: {pause_time:.0f}s...")
                    time.sleep(pause_time)

            # Fetch data
            bars = fetch_historical(client, ticker, start_date, end_date)

            if not bars:
                print(f"  No data returned — skipping")
                failed.append((ticker, "No data"))
                continue

            # Get or create symbol with metadata
            info = ticker_info.get(ticker, {})
            symbol_id = ensure_symbol(
                cursor, ticker,
                name=info.get("name"),
                sector=info.get("sector"),
            )

            # Insert into TimescaleDB
            rows = insert_ohlcv(cursor, symbol_id, bars)

            # Save to S3
            save_to_parquet_s3(bars, ticker)

            print(f"  ✓ {len(bars)} bars → DB + S3")
            success += 1

        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            failed.append((ticker, str(e)))
            # Don't stop — continue with next ticker
            continue

    # ── Summary ────────────────────────────────────────────────
    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"BACKFILL COMPLETE")
    print(f"{'='*60}")
    print(f"Time:     {int(elapsed // 60)}m {int(elapsed % 60)}s")
    print(f"Success:  {success}/{total}")
    print(f"Failed:   {len(failed)}/{total}")

    if failed:
        print(f"\nFailed tickers:")
        for ticker, error in failed:
            print(f"  {ticker}: {error}")

    # Verify database
    cursor.execute("SELECT COUNT(DISTINCT symbol_id) FROM daily_ohlcv;")
    unique_stocks = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM daily_ohlcv;")
    total_rows = cursor.fetchone()[0]

    print(f"\nDatabase:")
    print(f"  Unique stocks: {unique_stocks}")
    print(f"  Total rows:    {total_rows:,}")

    # Verify S3
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET, Prefix="daily_ohlcv/ticker="
    )
    s3_files = response.get("KeyCount", 0)
    print(f"  S3 Parquet files: {s3_files}")

    conn.close()
    print("\nDone!")