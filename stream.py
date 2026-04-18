"""
stream.py — Real-time tick streaming via WebSocket.

Connects to Massive.com's WebSocket API and receives
live trade data as it happens on the exchange.

Usage:
    python stream.py          → stream and print trades
    python stream.py --save   → stream and save to TimescaleDB
"""

import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage
from typing import List
import psycopg2
from psycopg2.extras import execute_values

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

# ── Configuration ──────────────────────────────────────────────────

TICKER = "AAPL"
SAVE_TO_DB = "--save" in sys.argv

BATCH_SIZE = 100
tick_buffer = []
tick_count = 0


def get_symbol_id(ticker):
    """Look up the symbol_id for a ticker."""
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT symbol_id FROM symbols WHERE ticker = %s;",
        (ticker,)
    )
    result = cursor.fetchone()
    conn.close()
    if result:
        return result[0]
    else:
        raise ValueError(f"Ticker {ticker} not found in symbols table!")


def flush_buffer(symbol_id):
    """
    Insert all ticks in the buffer to TimescaleDB.
    
    This is the micro-batch insert. We collect ticks in tick_buffer
    and flush them to the database when the buffer reaches BATCH_SIZE.
    
    execute_values sends all rows in one SQL statement — much faster
    than individual INSERTs.
    """
    global tick_buffer

    if not tick_buffer:
        return

    conn = psycopg2.connect(**db_config)
    try:
        cursor = conn.cursor()

        values = [
            (
                tick["time"],
                symbol_id,
                tick["price"],
                tick["size"],
                tick.get("conditions"),
                tick.get("exchange_id"),
            )
            for tick in tick_buffer
        ]

        execute_values(
            cursor,
            """
            INSERT INTO ticks (time, symbol_id, price, size, conditions, exchange_id)
            VALUES %s;
            """,
            values,
        )

        conn.commit()
        print(f"  [DB] Flushed {len(tick_buffer)} ticks to TimescaleDB")

    except Exception as e:
        conn.rollback()
        print(f"  [DB ERROR] {e}")
    finally:
        conn.close()

    tick_buffer = []


def handle_messages(messages: List[WebSocketMessage]):
   
    global tick_buffer, tick_count

    for msg in messages:
        # Each message has an event_type field.
        # "T" means it's a trade. Other types include:
        # "Q" = quote (bid/ask), "A" = aggregate bar
        
        if msg.event_type != "T":
            continue

        tick_count += 1

        # Extract the trade data
        tick = {
            "time": datetime.fromtimestamp(
                msg.timestamp / 1e9,  # Timestamps are in NANOseconds!
                tz=timezone.utc        # Not milliseconds like the REST API
            ),
            "price": msg.price,
            "size": msg.size,
            "conditions": getattr(msg, "conditions", None),
            "exchange_id": getattr(msg, "exchange", None),
        }

        # Print every trade to the terminal
        print(
            f"  #{tick_count:>6} | "
            f"{tick['time'].strftime('%H:%M:%S.%f')} | "
            f"${tick['price']:<10} | "
            f"Size: {tick['size']:>6} | "
            f"Exchange: {tick.get('exchange_id', '?')}"
        )

        # If saving to database, add to buffer
        if SAVE_TO_DB:
            tick_buffer.append(tick)

            # Flush when buffer is full
            if len(tick_buffer) >= BATCH_SIZE:
                flush_buffer(symbol_id)


# ── Main execution ─────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Real-time streaming: {TICKER}")
    print(f"Save to DB: {SAVE_TO_DB}")
    print("=" * 60)

    # Look up symbol_id if we're saving to DB
    if SAVE_TO_DB:
        symbol_id = get_symbol_id(TICKER)
        print(f"Symbol ID: {symbol_id}")

    print(f"\nConnecting to Massive.com WebSocket...")
    print(f"Subscribing to trades for {TICKER}")
    print(f"Press Ctrl+C to stop.\n")

    # Create the WebSocket client.
    # "T.AAPL" means "subscribe to Trades for AAPL"
    # The "T." prefix tells Massive we want trade data.
    # You could also use:
    #   "Q.AAPL" for quotes (bid/ask prices)
    #   "A.AAPL" for per-second aggregate bars
    #   "T.*" for ALL stock trades (don't do this on free tier!)
    ws = WebSocketClient(
        api_key=api_key,
        subscriptions=[f"T.{TICKER}"],
    )

    try:
        # ws.run() starts the WebSocket connection.
        # It connects, authenticates, subscribes, and then
        # calls handle_messages() every time data arrives.
        # This line BLOCKS — it runs forever until you Ctrl+C.
        ws.run(handle_msg=handle_messages)

    except KeyboardInterrupt:
        print(f"\n\nStopped. Received {tick_count} ticks total.")

        # Flush any remaining ticks in the buffer
        if SAVE_TO_DB and tick_buffer:
            symbol_id = get_symbol_id(TICKER)
            flush_buffer(symbol_id)
            print("Remaining ticks flushed to database.")