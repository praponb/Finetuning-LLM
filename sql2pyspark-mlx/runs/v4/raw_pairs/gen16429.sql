SELECT p.category, AVG(oi.quantity) avg_quantity
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
JOIN products p ON oi.product_id = p.id
GROUP BY p.category
ORDER BY avg_quantity DESC
