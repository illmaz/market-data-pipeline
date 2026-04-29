-- stg_symbols.sql
-- Cleans the raw symbols table.
-- Filters to only active stocks and standardizes column names.

SELECT
    symbol_id,
    ticker,
    UPPER(ticker)           AS ticker_upper,
    COALESCE(name, ticker)  AS company_name,
    COALESCE(sector, 'Unknown') AS sector,
    is_active,
    created_at::date        AS added_date
FROM {{ source('public', 'symbols') }}
WHERE is_active = TRUE
