WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, v
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        value
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) li AS value
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.signup_date,
        d.city
    FROM flattened a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT s.*, p.city AS customer_city
    FROM enriched s
    JOIN customers p
        ON s.customer_id = p.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    COUNT(*) FILTER (WHERE customer_city = 'Berlin') AS berlin_rows,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes_2
FROM enriched_carts
GROUP BY ROLLUP (DATE_TRUNC('HOUR', created_at))
ORDER BY span_minutes;
