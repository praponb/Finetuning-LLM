WITH order_stats AS (
    SELECT order_id, SUM(unit_price * quantity) AS revenue, AVG(unit_price) AS avg_unit_price
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.discount,
    o.customer_id,
    os.revenue,
    os.avg_unit_price
FROM orders o
JOIN order_stats os ON o.id = os.order_id
WHERE os.revenue >= 38
ORDER BY discount DESC;
