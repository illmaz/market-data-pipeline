{{ config(
    unique_key=['symbol_id', 'trade_date'],
    on_schema_change='sync_all_columns'
) }}

-- fct_volume_anomalies.sql
-- Flags trading days where volume is significantly above normal.
-- High volume often precedes large price moves — earnings surprises,
-- institutional accumulation, short squeezes, breaking news.
-- This is the automated version of what traders scan for manually.

WITH volume_stats AS (
    SELECT
        trade_date,
        symbol_id,
        close,
        volume,

        -- 30-day average volume: what's "normal" for this stock?
        ROUND(
            AVG(volume) OVER (
                PARTITION BY symbol_id
                ORDER BY trade_date
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ),
        0) AS avg_volume_30d,

        -- 30-day std deviation of volume: how much does volume vary?
        ROUND(
            STDDEV(volume) OVER (
                PARTITION BY symbol_id
                ORDER BY trade_date
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ),
        0) AS stddev_volume_30d

    FROM {{ ref('stg_daily_ohlcv') }}
    {% if is_incremental() %}
    WHERE trade_date >= (SELECT MAX(trade_date) - INTERVAL '60 days' FROM {{ this }})
    {% endif %}
),

anomalies AS (
    SELECT
        trade_date,
        symbol_id,
        close,
        volume,
        avg_volume_30d,
        stddev_volume_30d,

        -- Volume ratio: how many times normal volume is today?
        -- 2.0 means twice the normal volume
        ROUND(
            volume::numeric / NULLIF(avg_volume_30d, 0),
        2) AS volume_ratio,

        -- Z-score: how many standard deviations above normal?
        -- Z-score > 2 means statistically unusual
        ROUND(
            (volume - avg_volume_30d)::numeric / NULLIF(stddev_volume_30d, 0),
        2) AS volume_zscore,

        -- Anomaly classification
        CASE
            WHEN volume::numeric / NULLIF(avg_volume_30d, 0) >= 3.0 THEN 'extreme'
            WHEN volume::numeric / NULLIF(avg_volume_30d, 0) >= 2.0 THEN 'high'
            WHEN volume::numeric / NULLIF(avg_volume_30d, 0) >= 1.5 THEN 'elevated'
            ELSE 'normal'
        END AS volume_category

    FROM volume_stats
    WHERE avg_volume_30d IS NOT NULL  -- need 30 days of history
      AND volume::numeric / NULLIF(avg_volume_30d, 0) >= 1.5  -- only show elevated+
)

SELECT * FROM anomalies
