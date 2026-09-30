WITH parsed AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        TRANSFORM(items, item -> item.product_id) AS product_ids,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10
    FROM carts
    WHERE TO_DATE(created_at) < '2025-06-01'
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS last_seen,
    COUNT(DISTINCT id) AS distinct_ids
FROM parsed
GROUP BY activity_day
HAVING COUNT(DISTINCT id) >= 1
ORDER BY distinct_ids ASC NULLS LAST
