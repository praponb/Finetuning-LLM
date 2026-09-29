WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, attr_value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        key,
        value
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) x AS value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('week', created_at) AS created_at_period,
        DATE_TRUNC('week', created_at) AS created_at_period_2
    FROM lines
    WHERE created_at >= TIMESTAMP '2025-03-02 00:00:00'
      AND key IS NOT DISTINCT FROM 'utm_source'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(created_at AS DATE) AS created_at_day,
        DATEDIFF(CURRENT_DATE(), created_at_period) AS days_since_created_at_period
    FROM base
    WHERE value IS NOT NULL
)
-- Grouped metrics
SELECT
    value,
    SUM(days_since_created_at_period) FILTER (WHERE value = 'BUNDLE') AS bundle_total_days_since_created_at_period,
    MAX(created_at_period) AS last_created_at_period,
    ROUND(AVG(days_since_created_at_period), 2) AS avg_days_since_created_at_period
FROM cleaned
GROUP BY value
ORDER BY value DESC NULLS LAST;
