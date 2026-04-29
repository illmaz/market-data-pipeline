-- dim_symbols.sql
-- One row per stock with descriptive information and summary stats.
-- This is the main lookup table — joins sector, latest price,
-- data coverage, and performance summary in one place.
-- Use this when you need to know "what is this stock" not "what happened today"

WITH latest_prices AS (
    -- Get the most recent trading data for each stock
    SELECT DISTINCT ON (symbol_id)
        symbol_id,
        trade_date      AS latest_date,
        close           AS latest_close,
        volume          AS latest_volume
    FROM {{ ref('stg_daily_ohlcv') }}
    ORDER BY symbol_id, trade_date DESC
),

price_range AS (
    -- Calculate summary stats across all history
    SELECT
        symbol_id,
        MIN(trade_date)         AS earliest_date,
        MAX(trade_date)         AS latest_date,
        COUNT(*)                AS total_trading_days,
        MIN(close)              AS all_time_low,
        MAX(close)              AS all_time_high,
        ROUND(AVG(close), 2)    AS avg_close,
        ROUND(AVG(volume), 0)   AS avg_daily_volume
    FROM {{ ref('stg_daily_ohlcv') }}
    GROUP BY symbol_id
),

returns_summary AS (
    -- 30-day and 1-year return for each stock
    SELECT
        symbol_id,
        -- 30-day return: latest close vs close 30 trading days ago
        ROUND(
            (LAST_VALUE(close) OVER (
                PARTITION BY symbol_id
                ORDER BY trade_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) -
            NTH_VALUE(close, 30) OVER (
                PARTITION BY symbol_id
                ORDER BY trade_date DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )) /
            NULLIF(NTH_VALUE(close, 30) OVER (
                PARTITION BY symbol_id
                ORDER BY trade_date DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ), 0) * 100,
        2) AS return_30d_pct
    FROM {{ ref('stg_daily_ohlcv') }}
)

SELECT
    s.symbol_id,
    s.ticker,
    s.company_name,
    s.sector,
    s.added_date,

    -- Latest price info
    lp.latest_date,
    lp.latest_close,
    lp.latest_volume,

    -- Historical range
    pr.earliest_date,
    pr.total_trading_days,
    pr.all_time_low,
    pr.all_time_high,
    pr.avg_close,
    pr.avg_daily_volume,

    -- 30-day return (deduplicated — one row per stock)
    rs.return_30d_pct,

    -- How far is latest price from all-time high? (drawdown)
    ROUND(
        (lp.latest_close - pr.all_time_high) / NULLIF(pr.all_time_high, 0) * 100,
    2) AS pct_from_ath

FROM {{ ref('stg_symbols') }} s
LEFT JOIN latest_prices lp ON s.symbol_id = lp.symbol_id
LEFT JOIN price_range pr ON s.symbol_id = pr.symbol_id
LEFT JOIN (
    SELECT DISTINCT ON (symbol_id) symbol_id, return_30d_pct
    FROM returns_summary
) rs ON s.symbol_id = rs.symbol_id
