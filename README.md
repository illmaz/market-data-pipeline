# US Equity Market Data Pipeline

A production-grade data engineering pipeline that ingests, validates, stores, and serves daily OHLCV data for S&P 500 stocks.

Built as a portfolio project demonstrating skills relevant to data engineering roles in quantitative finance.

## Architecture
Massive.com API ──→ Python Pipeline ──→ TimescaleDB (hot storage)
──→ S3 Parquet  (cold storage)
──→ FastAPI     (data serving)
──→ Dashboard   (monitoring)

## Features

- **Daily OHLCV ingestion** for ~500 S&P 500 stocks via Massive.com Grouped Daily endpoint (one API call fetches all stocks)
- **TimescaleDB** hypertables with time-based partitioning for fast time-series queries
- **Parquet files on S3** for historical archive with Hive-style partitioning
- **5 automated data quality checks**: missing days, OHLC logic, price anomalies, volume anomalies, data freshness
- **Idempotent upserts** — pipeline is safely re-runnable with no duplicate data
- **Automated scheduling** via systemd timer (daily after market close)
- **API key authentication** — protected endpoints require `X-API-Key` header
- **Rate limiting** — 60 requests/minute per IP via slowapi
- **pgBouncer connection pooling** — `ThreadedConnectionPool` on port 6432
- **REST API** with FastAPI for querying OHLCV data by ticker and date range
- **Monitoring dashboard** showing pipeline health, top movers, sector coverage, and data quality
- **Pipeline audit trail** — every run is logged with row counts, timing, and error details

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12 |
| Database | PostgreSQL 16 + TimescaleDB |
| Connection Pooler | pgBouncer (port 6432) |
| Storage | AWS S3 (Parquet via PyArrow) |
| API | FastAPI + Uvicorn |
| Rate Limiting | slowapi |
| Data Source | Massive.com (formerly Polygon.io) |
| Scheduling | systemd timer |
| Infrastructure | AWS EC2 (Ubuntu 24.04) |
| Libraries | pandas, boto3, psycopg2, massive |

## Data Coverage

- ~500 S&P 500 stocks
- 2 years of daily history (~250,000+ data points)
- Updated daily after market close

## API Endpoints

| Endpoint | Auth Required | Description |
|----------|--------------|-------------|
| `GET /` | No | Dashboard with pipeline health and market overview |
| `GET /health` | No | Health check — verifies DB connectivity |
| `GET /api/ohlcv/{ticker}` | Yes | Daily OHLCV data with date range filtering |
| `GET /api/symbols` | Yes | List all tracked tickers with date coverage |
| `GET /api/pipeline/status` | Yes | Last 10 pipeline run results |
| `GET /api/dashboard/overview` | No | Pipeline stats and last run status |
| `GET /api/dashboard/movers` | No | Top gainers and losers by trading day |
| `GET /api/dashboard/sectors` | No | Sector breakdown with stock counts |
| `GET /api/dashboard/data-quality` | No | Data quality health report |

Protected endpoints require the `X-API-Key` header matching the `API_KEY` value in `.env`.

## Project Structure

```
├── main.py              # FastAPI app entrypoint — shared pool, auth, rate limiter
├── api.py               # OHLCV + symbols + pipeline status routes (auth-protected)
├── dashboard.py         # Monitoring dashboard routes + HTML frontend (public)
├── utils.py             # Shared DB helpers: ensure_symbol, fetch_ohlcv, insert_ohlcv, log_pipeline_run
├── ingest.py            # Single-stock daily ingestion
├── validate.py          # Standalone data quality checks
├── run_pipeline.py      # Daily pipeline — fetches grouped daily, inserts, validates, logs
├── backfill.py          # Single-stock historical backfill + S3
├── backfill_sp500.py    # Full S&P 500 backfill (2 years)
├── query_s3.py          # Read and analyze Parquet files from S3
├── stream.py            # WebSocket streaming handler (requires paid tier)
├── test_api.py          # API exploration and connection test
└── .env.example         # Template for environment variables
```

## Setup

1. Install PostgreSQL 16, TimescaleDB, and pgBouncer on Ubuntu
2. Create the `market_data` database and run the schema
3. Configure pgBouncer to listen on port 6432 and proxy to PostgreSQL
4. Set up AWS CLI and create an S3 bucket
5. Copy `.env.example` to `.env` and fill in credentials (including `API_KEY`)
6. Install dependencies: `pip install -r requirements.txt`
7. Run backfill: `python backfill_sp500.py`
8. Set up systemd timer to run `python run_pipeline.py` daily after market close
9. Start API: `uvicorn main:app --host 0.0.0.0 --port 8000`

## Design Decisions

**Single FastAPI entrypoint (main.py)**: `api.py` and `dashboard.py` are both `APIRouter` instances mounted into one app defined in `main.py`. This centralises the connection pool, API key verification, and rate limiter so they're not duplicated.

**Grouped Daily endpoint instead of per-ticker fetches**: The daily pipeline calls `get_grouped_daily_aggs(date)` once and gets all US stocks in a single response. The old approach looped over each ticker, burning rate-limit quota. One call covers everything.

**pgBouncer over direct PostgreSQL connections**: pgBouncer sits in front of PostgreSQL and multiplexes connections. The API uses a `ThreadedConnectionPool` (2–10 connections) through pgBouncer on port 6432, keeping idle connection count low under concurrent load.

**TimescaleDB over plain PostgreSQL**: Hypertables with monthly chunks provide automatic time-based partitioning. Queries filtered by date range only scan relevant chunks instead of the full table.

**Parquet over CSV for S3 storage**: Columnar format enables reading only the columns needed (e.g., just `close` prices). 20-75% smaller file sizes through type-aware compression.

**Upsert (ON CONFLICT DO UPDATE) over INSERT**: Makes the pipeline idempotent. Re-running after a failure fills gaps without creating duplicates.

**Overlapping fetch windows**: Daily pipeline fetches the last 4 days, not just today. Catches retroactive corrections from the data provider and auto-fills gaps from missed runs.

**Micro-batching for tick data**: WebSocket handler buffers ticks and flushes in batches of 100, reducing database round-trips from thousands/second to tens/second.
