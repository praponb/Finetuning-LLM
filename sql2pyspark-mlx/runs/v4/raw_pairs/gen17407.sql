WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    customer_id,
    MAX(created_at) AS last_created_at,
    COUNT(*) FILTER (WHERE code = 'SPRING10') AS spring10_rows,
    COUNT(DISTINCT code) AS distinct_codes
FROM flattened
GROUP BY 1, 2
ORDER BY hour_window DESC;
