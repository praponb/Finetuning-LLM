WITH order_stats AS (
    SELECT order_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM order_items
    GROUP BY order_id
)
SELECT o.store_id, os.unit_price_range
FROM orders o
LEFT JOIN order_stats os ON o.id = os.order_id
ORDER BY o.store_id
