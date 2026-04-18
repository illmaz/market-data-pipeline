import os 
from datetime import datetime, timedelta
from dotenv import load_dotenv
from massive import RESTClient

load_dotenv()

api_key = os.getenv("MASSIVE_API_KEY")

if not api_key:
    raise ValueError("MASSIVE_API_KEY not found in .env file!")

client = RESTClient(api_key=api_key)

ticker = "AAPL"

end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")

print(f"Fetching {ticker} daily OHLCV from {start_date} to {end_date}")
print("=" * 60)

aggs = []
for agg in client.list_aggs(
    ticker=ticker,
    multiplier=1,
    timespan="day",
    from_=start_date,
    to=end_date,
    adjusted=True,
    limit=50000,
):
    aggs.append(agg)

print(f"\nReceived {len(aggs)} bars\n")

for bar in aggs:
    print(f"Date:       {datetime.fromtimestamp(bar.timestamp / 1000).strftime('%Y-%m-%d')}")
    print(f"Open:       ${bar.open}")
    print(f"High:       ${bar.high}")
    print(f"Low:        ${bar.low}")
    print(f"Close:      ${bar.close}")
    print(f"Volume:     {bar.volume:,}")
    print(f"VWAP:       ${bar.vwap}")
    print(f"Num Trades: {bar.transactions}")
    print(f"Timestamp:  {bar.timestamp} (Unix ms)")
    print("-" * 40)

print("\n\nRaw first bar object (all attributes):")
if aggs:
    print(vars(aggs[0]))

