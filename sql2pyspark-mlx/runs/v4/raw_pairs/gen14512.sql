WITH order_stats AS (
    SELECT order_id, MAX(delivered_date) AS last_delivered_date, SUM(CASE WHEN carrier = 'UPS' THEN cost ELSE 0 END) AS ups_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.order_date,
    orders.status,
    order_stats.last_delivered_date,
    order_stats.ups_cost
FROM orders
LEFT JOIN order_stats ON orders.id = order_stats.order_id
WHERE order_stats.ups_cost >= 40
ORDER BY status;
