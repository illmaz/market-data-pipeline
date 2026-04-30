from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values


def ensure_symbol(cursor, ticker):

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
        ).replace(hour=0, minute=0, second=0, microsecond=0),
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
    if not bars:
        print("No bars to insert")
        return 0

    values = [
        (
            bar["time"],
            symbol_id,
            bar["open"],
            bar["high"],
            bar["low"],
            bar["close"],
            bar["volume"],
            bar["vwap"],
            bar["num_trades"],
            "massive",  # data source
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


def log_pipeline_run(cursor, run_type, status, rows_fetched, rows_inserted,
                     error_message=None, metadata=None):

    cursor.execute(
        """
        INSERT INTO pipeline_runs
            (run_type, status, finished_at, rows_fetched, rows_inserted,
             error_message, metadata)
        VALUES (%s, %s, NOW(), %s, %s, %s, %s)
        RETURNING run_id;
        """,
        (run_type, status, rows_fetched, rows_inserted,
         error_message, psycopg2.extras.Json(metadata))
    )
    return cursor.fetchone()[0]

def fetch_grouped_daily(client, date_str):
    """
    Fetch OHLCV for ALL US stocks for a single date.
    One API call returns everything — no looping, no rate limits.
    
    Returns a dict keyed by ticker: {"AAPL": {bar data}, "MSFT": {bar data}, ...}
    """
    results = {}
    for agg in client.get_grouped_daily_aggs(date_str):
        results[agg.ticker] = {
            "time": datetime.fromtimestamp(
            agg.timestamp / 1000, tz=timezone.utc
            ).replace(hour=0, minute=0, second=0, microsecond=0),
            "open": agg.open,
            "high": agg.high,
            "low": agg.low,
            "close": agg.close,
            "volume": int(agg.volume),
            "vwap": getattr(agg, 'vwap', None),
            "num_trades": getattr(agg, 'transactions', None),
        }
    return results