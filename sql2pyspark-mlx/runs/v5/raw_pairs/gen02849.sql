WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, v
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('MONTH', created_at) created_at_period
    FROM exploded
    WHERE created_at > DATE '2025-03-01'
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.coupon_codes,
        e.key,
        e.v,
        e.created_at_period,
        b.signup_date,
        b.city,
        b.country
    FROM base e
    INNER JOIN customers b
        ON e.customer_id <=> b.id
    WHERE b.city <> 'London'
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.name,
        d.city AS customer_city
    FROM joined a
    LEFT JOIN customers d
        ON a.customer_id = d.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', created_at_period) AS year_start,
    customer_id,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    COUNT(*) FILTER (WHERE customer_city = 'Berlin') AS berlin_rows,
    (UNIX_TIMESTAMP(MAX(created_at_period)) - UNIX_TIMESTAMP(MIN(created_at_period))) / 60 AS span_minutes,
    COUNT(CASE WHEN customer_city = 'Berlin' THEN 1 END) AS berlin_count,
    SIZE(COLLECT_SET(country)) AS num_countries
FROM joined_carts
GROUP BY ROLLUP (DATE_TRUNC('YEAR', created_at_period), customer_id)
ORDER BY jp_count DESC NULLS LAST
