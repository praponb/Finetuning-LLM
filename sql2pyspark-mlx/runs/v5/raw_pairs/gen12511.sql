WITH order_stats AS (
    SELECT order_id, COUNT(DISTINCT quantity) AS unique_orders
    FROM order_items
    WHERE unit_price >= 260
    GROUP BY order_id
)
SELECT orders.id, orders.order_date, order_stats.unique_orders
FROM orders
LEFT OUTER JOIN order_stats ON orders.id = order_stats.order_id
WHERE order_stats.unique_orders > 5
ORDER BY order_date DESC;
