WITH order_stats AS (
    SELECT order_id, MIN(paid_at) AS min_paid_at, SUM(amount) AS total_amount
    FROM payments
    WHERE TO_DATE(paid_at) < '2025-10-29'
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.order_date,
    orders.status,
    order_stats.min_paid_at,
    order_stats.total_amount
FROM orders
LEFT JOIN order_stats ON orders.id = order_stats.order_id
