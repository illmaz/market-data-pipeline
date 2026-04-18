# US Equity Market Data Pipeline

A production-grade data engineering pipeline that ingests, validates, stores, and serves daily OHLCV data for S&P 500 stocks.

Built as a portfolio project demonstrating skills relevant to data engineering roles in quantitative finance.

## Architecture
Massive.com API ──→ Python Pipeline ──→ TimescaleDB (hot storage)
──→ S3 Parquet  (cold storage)
──→ FastAPI     (data serving)
──→ Dashboard   (monitoring)

## Features

- **Daily OHLCV ingestion** for ~500 S&P 500 stocks via Massive.com API
- **TimescaleDB** hypertables with time-based partitioning for fast time-series queries
- **Parquet files on S3** for historical archive with Hive-style partitioning
- **5 automated data quality checks**: missing days, OHLC logic, price anomalies, volume anomalies, data freshness
- **Idempotent upserts** — pipeline is safely re-runnable with no duplicate data
- **Automated scheduling** via APScheduler (daily at 22:30 UTC / 6:30 PM ET)
- **REST API** with FastAPI for querying OHLCV data by ticker and date range
- **Monitoring dashboard** showing pipeline health, top movers, sector coverage, and data quality
- **Pipeline audit trail** — every run is logged with row counts, timing, and error details

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12 |
| Database | PostgreSQL 16 + TimescaleDB |
| Storage | AWS S3 (Parquet via PyArrow) |
| API | FastAPI + Uvicorn |
| Data Source | Massive.com (formerly Polygon.io) |
| Scheduling | APScheduler |
| Infrastructure | AWS EC2 (Ubuntu 24.04) |
| Libraries | pandas, boto3, psycopg2, massive |

## Data Coverage

- ~500 S&P 500 stocks
- 2 years of daily history (~250,000+ data points)
- Updated daily after market close

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Dashboard with pipeline health and market overview |
| `GET /api/ohlcv/{ticker}` | Daily OHLCV data with date range filtering |
| `GET /api/symbols` | List all tracked tickers with date coverage |
| `GET /api/dashboard/overview` | Pipeline stats and last run status |
| `GET /api/dashboard/movers` | Top gainers and losers by trading day |
| `GET /api/dashboard/sectors` | Sector breakdown with stock counts |
| `GET /api/dashboard/data-quality` | Data quality health report |

## Project Structure

├── api.py               # FastAPI REST endpoints for OHLCV data
├── dashboard.py          # Monitoring dashboard with HTML frontend
├── ingest.py             # Single-stock daily ingestion
├── validate.py           # Standalone data quality checks
├── run_pipeline.py       # Automated daily pipeline with scheduler
├── backfill.py           # Single-stock historical backfill + S3
├── backfill_sp500.py     # Full S&P 500 backfill (2 years)
├── query_s3.py           # Read and analyze Parquet files from S3
├── stream.py             # WebSocket streaming handler (requires paid tier)
├── test_api.py           # API exploration and connection test
└── .env.example          # Template for environment variables

## Setup

1. Install PostgreSQL 16 and TimescaleDB on Ubuntu
2. Create the `market_data` database and run the schema
3. Set up AWS CLI and create an S3 bucket
4. Copy `.env.example` to `.env` and fill in credentials
5. Install dependencies: `pip install -r requirements.txt`
6. Run backfill: `python backfill_sp500.py`
7. Start daily scheduler: `python run_pipeline.py schedule`
8. Start API: `uvicorn dashboard:app --host 0.0.0.0 --port 8000`

## Design Decisions

**TimescaleDB over plain PostgreSQL**: Hypertables with monthly chunks provide automatic time-based partitioning. Queries filtered by date range only scan relevant chunks instead of the full table.

**Parquet over CSV for S3 storage**: Columnar format enables reading only the columns needed (e.g., just `close` prices). 20-75% smaller file sizes through type-aware compression.

**Upsert (ON CONFLICT DO UPDATE) over INSERT**: Makes the pipeline idempotent. Re-running after a failure fills gaps without creating duplicates.

**Overlapping fetch windows**: Daily pipeline fetches the last 7 days, not just today. Catches retroactive corrections from the data provider and auto-fills gaps from missed runs.

**Micro-batching for tick data**: WebSocket handler buffers ticks and flushes in batches of 100, reducing database round-trips from thousands/second to tens/second.

