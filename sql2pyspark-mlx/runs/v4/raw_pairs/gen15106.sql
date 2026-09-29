WITH flattened AS (
    SELECT
        id,
        created_at,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, value
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    MIN(created_at) AS first_created_at,
    MIN(created_at) AS first_created_at_2,
    COUNT(*) FILTER (WHERE k = 'device') AS device_rows,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE k = 'gift') AS gift_rows
FROM flattened
GROUP BY 1;
