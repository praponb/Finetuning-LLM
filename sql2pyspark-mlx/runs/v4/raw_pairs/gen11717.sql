SELECT o.status, COUNT(DISTINCT oi.quantity) AS unique_orders, SUM(oi.unit_price * oi.quantity) AS revenue
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.product_id IS NOT NULL
GROUP BY o.status
HAVING SUM(oi.unit_price * oi.quantity) >= 57
