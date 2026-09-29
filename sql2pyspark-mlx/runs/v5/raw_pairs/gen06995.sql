SELECT p.category, COUNT(DISTINCT oi.unit_price) AS unique_orders, COUNT(*) AS total_count
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN orders o ON oi.order_id = o.id
GROUP BY p.category
ORDER BY total_count DESC
