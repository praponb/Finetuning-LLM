WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS element
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(created_at AS DATE) AS created_at_day,
        DATE_TRUNC('week', created_at) AS created_at_period,
        COALESCE(element, 'NONE') AS element_filled
    FROM flattened
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 13:00:00')
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        created_at_day,
        created_at_period,
        element_filled,
        TO_DATE(created_at) AS created_at_date
    FROM filtered
    WHERE element = 'VIP'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at_day,
        created_at_period,
        element_filled,
        ADD_MONTHS(created_at_date, 6) AS created_at_date_plus_months
    FROM filtered_carts
    WHERE TO_DATE(created_at) >= '2024-11-01'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', created_at_date_plus_months) AS year_start,
    COUNT(*) AS num_rows,
    MIN(created_at_period) AS started_at,
    COUNT(*) AS row_count,
    MIN(created_at_period) AS started_at_2,
    (UNIX_TIMESTAMP(MAX(created_at_period)) - UNIX_TIMESTAMP(MIN(created_at_period))) / 60 AS span_minutes,
    MAX(created_at_period) AS last_seen
FROM base
GROUP BY year_start
ORDER BY num_rows ASC NULLS FIRST
