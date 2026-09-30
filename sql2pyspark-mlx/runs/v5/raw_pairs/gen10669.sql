WITH by_dimension AS (
    -- Join a dimension and aggregate
    SELECT
        o.channel,
        e.method,
        COUNT(*) AS cnt,
        SUM(e.amount) AS total_amount
    FROM payments e
    INNER JOIN orders o ON e.order_id = o.id
    GROUP BY o.channel, e.method
)
SELECT
    channel,
    method,
    cnt,
    total_amount
FROM by_dimension
ORDER BY method, channel
