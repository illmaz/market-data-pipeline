import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from massive import RESTClient
import psycopg2
from utils import ensure_symbol, fetch_ohlcv, insert_ohlcv, log_pipeline_run

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

# -- Main execution ----------------

if __name__ == "__main__":
    ticker = "AAPL"

    # Fetch the last 5 trading days (using 10 calendar days for safety)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")

    print(f"Pipeline starting: {ticker} from {start_date} to {end_date}")
    print("=" * 60)

    # Initialize API client
    client = RESTClient(api_key=api_key)

    # Connect to database
    # 'with' ensures the connection is closed even if an error occurs.
    # This prevents "connection leak" — abandoned connections that pile
    # up and eventually prevent new connections.
    conn = psycopg2.connect(**db_config)

    try: 
        conn.autocommit = False
        cursor = conn.cursor()

        # Step 1: Ensure the symbol exists in our symbols table
        symbol_id = ensure_symbol(cursor, ticker)
        print(f"Symbol: {ticker} (id={symbol_id})")

        # Step 2: Fetch data from API
        print(f"Fetching data from Massive.com...")
        bars = fetch_ohlcv(client, ticker, start_date, end_date)
        print(f"Fetched {len(bars)} bars")

        # Step 3: Insert into database
        print(f"Inserting into TimescaleDB...")
        rows_inserted = insert_ohlcv(cursor, symbol_id, bars)
        print(f"Inserted/updated {rows_inserted} rows")

        # Step 4: Log the pipeline run
        run_id = log_pipeline_run(
            cursor,
            run_type="daily_ohlcv",
            status="success",
            rows_fetched=len(bars),
            rows_inserted=rows_inserted,
            metadata={"ticker": ticker, "start": start_date, "end": end_date}
        )
        print(f"Pipeline run logged (run_id={run_id})")

        # Step 5: Commit the transaction
        # Nothing is actually written to disk until this line.
        # All the inserts above are "pending" in the transaction.
        conn.commit()
        print("\nSUCCESS — Transaction committed!")

    except Exception as e:
        # If ANYTHING goes wrong, roll back the entire transaction.
        # This keeps the database in a clean state.
        conn.rollback()
        print(f"\nFAILED — Transaction rolled back!")
        print(f"Error: {e}")

        # Still log the failure so we have a record
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
            print("Could not log pipeline failure to database.")

        raise  # Re-raise so we see the full error traceback

    finally:
        conn.close()
        print("Database connection closed.")

    # ── Verify the data ────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("VERIFICATION — Reading back from database:")
    print("=" * 60)

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            d.time::date AS trade_date,
            s.ticker,
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
        ORDER BY d.time DESC
        LIMIT 5;
    """, (ticker,))

    rows = cursor.fetchall()
    print(f"\nLatest {len(rows)} rows for {ticker}:\n")

    for row in rows:
        print(f"  {row[0]} | O:{row[2]:.2f} H:{row[3]:.2f} "
              f"L:{row[4]:.2f} C:{row[5]:.2f} | Vol:{row[6]:>12,} | "
              f"VWAP:{row[7]:.2f} | Trades:{row[8]:,}")

    conn.close()
