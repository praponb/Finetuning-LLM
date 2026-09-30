WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS code
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    customer_id,
    code,
    SIZE(COLLECT_SET(code)) AS num_codes,
    MAX(created_at) AS ended_at,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    COUNT(CASE WHEN code = 'SPRING10' THEN 1 END) AS spring10_count,
    MIN(created_at) AS first_seen,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list,
    RANK() OVER (PARTITION BY DATE_TRUNC('DAY', created_at) ORDER BY COUNT(CASE WHEN code = 'SPRING10' THEN 1 END) DESC) AS spring10_count_rank
FROM exploded
GROUP BY DATE_TRUNC('DAY', created_at), customer_id, code
ORDER BY spring10_count DESC NULLS FIRST
