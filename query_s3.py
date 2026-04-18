"""
query_s3.py — Read Parquet files from S3 and run analysis.

This demonstrates querying your archived data on S3 without
touching the database. Useful for:
- Heavy analysis that would slow down your database
- Sharing data with teammates who don't have DB access
- Using tools like Jupyter notebooks for research

Think of it this way:
- TimescaleDB = your fast, live data (for the API and dashboards)
- S3 Parquet = your archive (for research and analysis)
"""

import os
import io
from dotenv import load_dotenv
import boto3
import pandas as pd

load_dotenv()

S3_BUCKET = "market-data-pipeline-illmaz"


def read_parquet_from_s3(ticker):
   
    s3_client = boto3.client("s3")
    s3_key = f"daily_ohlcv/ticker={ticker}/data.parquet"

    print(f"Reading s3://{S3_BUCKET}/{s3_key}...")

    # get_object returns the file contents in response["Body"]
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)

    # Read the bytes into a pandas DataFrame
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))

    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"Date range: {df['time'].min()} to {df['time'].max()}")
    print(f"Columns: {list(df.columns)}")

    return df


def analyze(df, ticker):
    """
    Run some basic financial analysis on the data.
    
    This shows why having 2 years of history matters —
    you can calculate meaningful statistics that require
    lots of data points.
    """
    print(f"\n{'='*60}")
    print(f"Analysis: {ticker} ({len(df)} trading days)")
    print(f"{'='*60}")

    # ── Price summary ──────────────────────────────────────────
    # df["close"].describe() gives count, mean, std, min, max, quartiles
    # This is a pandas built-in — one line for a full statistical summary
    print(f"\nPrice Summary (Close):")
    print(f"  Current:  ${df['close'].iloc[-1]:.2f}")
    print(f"  Average:  ${df['close'].mean():.2f}")
    print(f"  Highest:  ${df['close'].max():.2f}")
    print(f"  Lowest:   ${df['close'].min():.2f}")

    # ── Daily returns ──────────────────────────────────────────
    # "Return" = how much the price changed as a percentage.
    # pct_change() calculates (today - yesterday) / yesterday for each row.
    # Example: if close was $100 yesterday and $105 today,
    # pct_change = (105-100)/100 = 0.05 = 5%
    df["daily_return"] = df["close"].pct_change()

    print(f"\nDaily Returns:")
    print(f"  Average:  {df['daily_return'].mean()*100:.3f}%")
    print(f"  Std Dev:  {df['daily_return'].std()*100:.3f}%")
    print(f"  Best Day: {df['daily_return'].max()*100:.2f}%")
    print(f"  Worst Day:{df['daily_return'].min()*100:.2f}%")

    # ── Volume analysis ────────────────────────────────────────
    print(f"\nVolume:")
    print(f"  Average:  {df['volume'].mean():,.0f} shares/day")
    print(f"  Highest:  {df['volume'].max():,.0f}")
    print(f"  Lowest:   {df['volume'].min():,.0f}")

    # ── Monthly performance ────────────────────────────────────
    # resample('ME') groups data by month.
    # For each month, we take the last closing price.
    # Then pct_change() gives us the monthly return.
    #
    # This is why time-series data needs proper datetime indexes.
    # pandas makes this kind of analysis trivially easy.
    df = df.set_index("time")
    monthly = df["close"].resample("ME").last().pct_change().dropna()

    print(f"\nMonthly Returns (last 6 months):")
    for date, ret in monthly.tail(6).items():
        print(f"  {date.strftime('%Y-%m')}: {ret*100:+.2f}%")

    # ── Volatility ─────────────────────────────────────────────
    # Volatility measures how much the price jumps around.
    # Higher volatility = riskier stock.
    # Annualized volatility = daily std dev × sqrt(252)
    # Why 252? That's the number of trading days in a year.
    # This is a standard formula used by every quant on Wall Street.
    import math
    annual_vol = df["daily_return"].std() * math.sqrt(252) * 100
    print(f"\nAnnualized Volatility: {annual_vol:.1f}%")
    print(f"  (For reference: S&P 500 is typically 15-20%)")


# ── Main execution ─────────────────────────────────────────────────

if __name__ == "__main__":
    ticker = "AAPL"

    # Read from S3 — no database involved!
    df = read_parquet_from_s3(ticker)

    # Run analysis
    analyze(df, ticker)