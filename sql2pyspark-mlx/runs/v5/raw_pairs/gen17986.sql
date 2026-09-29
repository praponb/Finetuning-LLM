WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS tag
),
matched_keys AS (
    SELECT customer_id, id
    FROM flattened
    INTERSECT ALL
    SELECT customer_id, id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
verified AS (
    SELECT s.*
    FROM flattened s
    JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
joined AS (
    SELECT
        s.*,
        b.email,
        b.age
    FROM verified s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
),
cleaned AS (
    SELECT
        customer_id,
        created_at,
        tag,
        CONCAT(email, ', ', tag) AS email_tag
    FROM joined
    WHERE customer_id IS NOT NULL
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    MAX(created_at) AS last_seen,
    COUNT(CASE WHEN tag = 'VIP' THEN 1 END) AS vip_count,
    COUNT(CASE WHEN tag = 'SPRING10' THEN 1 END) AS spring10_count,
    COUNT_IF(tag = 'SPRING10') AS spring10_events,
    MIN(created_at) AS first_seen,
    COUNT(*) AS num_rows,
    GROUPING_ID() AS grouping_level
FROM cleaned
GROUP BY ROLLUP (DATE_TRUNC('DAY', created_at))
ORDER BY num_rows ASC NULLS LAST;
