{{ config(
    unique_key=['symbol_id', 'trade_date'],
    on_schema_change='sync_all_columns'
) }}

-- fct_moving_averages.sql
-- Calculates 20-day and 50-day moving averages for each stock.
-- Moving averages are the most fundamental technical indicator —
-- used to identify trends and generate trading signals.

WITH moving_avgs AS (
    SELECT
        trade_date,
        symbol_id,
        close,
        volume,

        -- 20-day moving average: average of last 20 trading days
        ROUND(
            AVG(close) OVER (
                PARTITION BY symbol_id
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ),
        2) AS ma_20,

        -- 50-day moving average: average of last 50 trading days
        ROUND(
            AVG(close) OVER (
                PARTITION BY symbol_id
                ORDER BY trade_date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ),
        2) AS ma_50,

        -- 20-day average volume: is today's volume above or below normal?
        ROUND(
            AVG(volume) OVER (
                PARTITION BY symbol_id
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ),
        0) AS avg_volume_20d,

        -- How many rows do we have for this stock up to this date?
        -- Used to filter out stocks with insufficient history
        ROW_NUMBER() OVER (
            PARTITION BY symbol_id
            ORDER BY trade_date
        ) AS trading_days_count

    FROM {{ ref('stg_daily_ohlcv') }}
    {% if is_incremental() %}
    WHERE trade_date >= (SELECT MAX(trade_date) - INTERVAL '90 days' FROM {{ this }})
    {% endif %}
),

signals AS (
    SELECT
        trade_date,
        symbol_id,
        close,
        volume,
        ma_20,
        ma_50,
        avg_volume_20d,
        trading_days_count,

        -- Is price above or below each moving average?
        CASE
            WHEN close > ma_20 THEN 'above'
            WHEN close < ma_20 THEN 'below'
            ELSE 'at'
        END AS price_vs_ma20,

        CASE
            WHEN close > ma_50 THEN 'above'
            WHEN close < ma_50 THEN 'below'
            ELSE 'at'
        END AS price_vs_ma50,

        -- Golden cross / death cross signal
        -- Compares today's MA relationship to yesterday's
        CASE
            WHEN ma_20 > ma_50
             AND LAG(ma_20) OVER (PARTITION BY symbol_id ORDER BY trade_date)
              <= LAG(ma_50) OVER (PARTITION BY symbol_id ORDER BY trade_date)
            THEN 'golden_cross'
            WHEN ma_20 < ma_50
             AND LAG(ma_20) OVER (PARTITION BY symbol_id ORDER BY trade_date)
              >= LAG(ma_50) OVER (PARTITION BY symbol_id ORDER BY trade_date)
            THEN 'death_cross'
            ELSE 'none'
        END AS crossover_signal,

        -- Volume vs 20-day average (how unusual is today's volume?)
        ROUND(volume::numeric / NULLIF(avg_volume_20d, 0), 2) AS volume_ratio

    FROM moving_avgs
    WHERE trading_days_count >= 20  -- only show rows with enough history for MA20
)

SELECT * FROM signals
