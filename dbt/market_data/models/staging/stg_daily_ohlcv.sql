-- stg_daily_ohlcv.sql
-- Cleans and standardizes the raw daily_ohlcv table.
-- Casts timestamp to date, renames columns for clarity,
-- and filters out any rows with invalid prices.

SELECT
    time::date                AS trade_date,
    symbol_id,
    open::numeric(12,4)       AS open,
    high::numeric(12,4)       AS high,
    low::numeric(12,4)        AS low,
    close::numeric(12,4)      AS close,
    volume::bigint            AS volume,
    vwap::numeric(12,4)       AS vwap,
    num_trades,
    source
FROM {{ source('public', 'daily_ohlcv') }}
WHERE close > 0
  AND open > 0
  AND high > 0
  AND low > 0
  AND volume > 0
