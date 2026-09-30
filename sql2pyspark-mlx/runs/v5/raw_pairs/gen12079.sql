WITH exploded AS (
    SELECT
        id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS attr_key, value
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    attr_key,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    COUNT(CASE WHEN attr_key = 'gift' THEN 1 END) AS gift_count,
    RANK() OVER (PARTITION BY DATE_TRUNC('DAY', created_at) ORDER BY (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 DESC) AS duration_min_rank
FROM exploded
GROUP BY activity_day, attr_key
ORDER BY attr_key DESC
