WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, value
)
SELECT
    k,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    MIN(created_at) AS started_at,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    MIN(created_at) AS first_created_at
FROM flattened
GROUP BY 1, 2, 3;
