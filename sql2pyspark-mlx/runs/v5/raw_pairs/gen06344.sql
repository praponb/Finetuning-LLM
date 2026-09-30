WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.order_id IN (6, 22)
        )
)
-- Grouped metrics
SELECT
    status,
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    MAX(discount) FILTER (WHERE status = 'SHIPPED') AS shipped_max_discount
FROM eligible
GROUP BY status, DATE_TRUNC('QUARTER', order_date)
ORDER BY shipped_max_discount DESC NULLS FIRST;
