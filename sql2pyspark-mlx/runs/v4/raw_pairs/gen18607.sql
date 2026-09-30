WITH order_stats AS (
    SELECT order_id, COUNT(DISTINCT unit_price) AS unique_orders, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM order_items
    GROUP BY order_id
)
SELECT o.customer_id, os.unique_orders, os.avg_quantity
FROM orders o
JOIN order_stats os ON o.id = os.order_id
