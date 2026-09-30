WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        created_at,
        ship_to,
        attributes,
        COALESCE(attributes['gift'], 'none') AS gift_or_default,
        attributes['gift'] AS gift
    FROM carts
    WHERE created_at < '2023-04-19'
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    COUNT(*) AS event_count,
    MAX(created_at) AS last_seen,
    COUNT(*) AS num_rows,
    COUNT(*) AS cnt
FROM unpacked
GROUP BY customer_id
ORDER BY customer_id
