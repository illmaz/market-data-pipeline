"""
backfill.py — Historical backfill: fetch 2 years of OHLCV data,
store in TimescaleDB AND as Parquet files on S3.
"""

import os
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from massive import RESTClient
import psycopg2
from psycopg2.extras import execute_values, Json
import pandas as pd
import boto3
import sys

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

S3_BUCKET = os.getenv("S3_BUCKET", "market-data-pipeline-illmaz")

# ── Reused functions from ingest.py ────────────────────────────────

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

# ── Fetch historical data ──────────────────────────────────────────

def fetch_historical(client, ticker, start_date, end_date):
    """
    Fetch all daily OHLCV bars for a ticker over a date range.
    
    This works for any range — 10 days or 2 years. The Massive API
    handles pagination automatically through the client library.
    We just iterate and collect all the bars.
    """
    print(f"  Fetching {ticker} from {start_date} to {end_date}...")

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
            "time": datetime.fromtimestamp(agg.timestamp / 1000, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            "ticker": ticker,
            "open": agg.open,
            "high": agg.high,
            "low": agg.low,
            "close": agg.close,
            "volume": int(agg.volume),
            "vwap": agg.vwap,
            "num_trades": agg.transactions,
        })

    print(f"  Got {len(bars)} bars")
    return bars

# ── Parquet + S3 ───────────────────────────────────────────────────

def save_to_parquet_s3(bars, ticker):
    """
    Convert bars to a Parquet file and upload to S3.
    
    The process:
    1. Convert list of dicts → pandas DataFrame
    2. Save DataFrame as Parquet file locally (temporary)
    3. Upload Parquet file to S3
    4. Delete local temp file
    
    S3 path structure: s3://bucket/daily_ohlcv/ticker=AAPL/data.parquet
    
    Why this path structure? It's called "Hive partitioning."
    Tools like AWS Athena and Spark can read this structure and
    automatically understand that "ticker=AAPL" means this file
    only contains AAPL data. This makes queries like
    "give me only MSFT data" fast because the tool skips all
    files that aren't in the ticker=MSFT folder.
    """
    if not bars:
        print("  No bars to save.")
        return

    df = pd.DataFrame(bars)
    
    df["time"] = pd.to_datetime(df["time"])

    # Save as Parquet locally
    local_path = f"/tmp/{ticker}_daily_ohlcv.parquet"

    df.to_parquet(
        local_path,
        engine="pyarrow",  
        index=False,        
    )

   
    parquet_size = os.path.getsize(local_path)
    csv_size = len(df.to_csv(index=False).encode())
    compression_ratio = (1 - parquet_size / csv_size) * 100

    print(f"  Parquet file: {parquet_size:,} bytes")
    print(f"  CSV would be: {csv_size:,} bytes")
    print(f"  Compression:  {compression_ratio:.1f}% smaller")

    # Upload to S3
    s3_client = boto3.client("s3")
    s3_key = f"daily_ohlcv/ticker={ticker}/data.parquet"

    s3_client.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"  Uploaded to s3://{S3_BUCKET}/{s3_key}")

    # Clean up the temp file
    os.remove(local_path)

    return s3_key

# ── Retry logic ────────────────────────────────────────────────────

def fetch_with_retry(fetch_func, max_retries=3):
    """
    Retry a function with exponential backoff.
    Attempts: 3 total. Wait times: 5s, 10s, 20s.
    """
    for attempt in range(max_retries):
        try:
            return fetch_func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            wait = 5 * (2 ** attempt)
            print(f"  Attempt {attempt + 1} failed: {e}")
            print(f"  Retrying in {wait} seconds...")
            time.sleep(wait)

# ── Main execution ─────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backfill.py <TICKER>")
        print("Example: python backfill.py MSFT")
        sys.exit(1)

    ticker = sys.argv[1].upper()

    # 2 years of history
    now_utc = datetime.now(tz=timezone.utc)
    end_date = now_utc.strftime("%Y-%m-%d")
    start_date = (now_utc - timedelta(days=730)).strftime("%Y-%m-%d")

    print(f"Historical backfill: {ticker}")
    print(f"Range: {start_date} to {end_date}")
    print("=" * 60)

    # ── Step 1: Fetch from API ─────────────────────────────────────
    client = RESTClient(api_key=api_key)
    bars = fetch_with_retry(
        lambda: fetch_historical(client, ticker, start_date, end_date)
    )

    if not bars:
        print("No data returned. Exiting.")
        exit(1)

    # ── Step 2: Insert into TimescaleDB ────────────────────────────
    print("\nInserting into TimescaleDB...")
    conn = psycopg2.connect(**db_config)

    try:
        conn.autocommit = False
        cursor = conn.cursor()

        symbol_id = ensure_symbol(cursor, ticker)
        rows_inserted = insert_ohlcv(cursor, symbol_id, bars)

        conn.commit()
        print(f"  Inserted/updated {rows_inserted} rows")

    except Exception as e:
        conn.rollback()
        print(f"  Database error: {e}")
        raise
    finally:
        conn.close()

    # ── Step 3: Save as Parquet and upload to S3 ───────────────────
    print("\nSaving to Parquet and uploading to S3...")
    s3_key = save_to_parquet_s3(bars, ticker)

    # ── Step 4: Verify ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)

    # Verify database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*), MIN(time)::date, MAX(time)::date
        FROM daily_ohlcv d
        JOIN symbols s ON d.symbol_id = s.symbol_id
        WHERE s.ticker = %s;
    """, (ticker,))
    count, min_date, max_date = cursor.fetchone()
    print(f"\nTimescaleDB: {count} rows, {min_date} to {max_date}")
    conn.close()

    # Verify S3
    s3_client = boto3.client("s3")
    response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
    s3_size = response["ContentLength"]
    print(f"S3: s3://{S3_BUCKET}/{s3_key} ({s3_size:,} bytes)")

    print("\nBackfill complete!")