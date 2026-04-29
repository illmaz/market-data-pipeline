-- fct_daily_returns.sql
-- Calculates the daily percentage return for each stock.
-- Uses LAG() window function to get the previous trading day's close price.
-- This is the foundation for momentum signals, performance tracking,
-- and risk calculations.

WITH base AS (
    SELECT
        trade_date,
        symbol_id,
        open,
        high,
        low,
        close,
        volume,
        vwap,
        LAG(close) OVER (
            PARTITION BY symbol_id
            ORDER BY trade_date
        ) AS prev_close
    FROM {{ ref('stg_daily_ohlcv') }}
),

returns AS (
    SELECT
        trade_date,
        symbol_id,
        open,
        high,
        low,
        close,
        prev_close,
        volume,
        vwap,
        -- Daily return: (today - yesterday) / yesterday * 100
        -- NULLIF prevents division by zero if prev_close is somehow 0
        ROUND(
            (close - prev_close) / NULLIF(prev_close, 0) * 100,
        2) AS daily_return_pct,

        -- Intraday return: how much did it move during the day?
        ROUND(
            (close - open) / NULLIF(open, 0) * 100,
        2) AS intraday_return_pct,

        -- Was it a positive or negative day?
        CASE
            WHEN close > prev_close THEN 'up'
            WHEN close < prev_close THEN 'down'
            ELSE 'flat'
        END AS day_direction
    FROM base
    WHERE prev_close IS NOT NULL  -- exclude first row per stock (no previous day)
)

SELECT * FROM returns
