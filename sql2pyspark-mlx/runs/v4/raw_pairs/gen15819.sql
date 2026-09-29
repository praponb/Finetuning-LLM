WITH flattened AS (
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS value
    FROM carts
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    value,
    SIZE(COLLECT_SET(value)) AS num_values,
    COUNT(CASE WHEN value = 'SPRING10' THEN 1 END) AS spring10_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM flattened
GROUP BY hour_window, value
ORDER BY num_values DESC;
