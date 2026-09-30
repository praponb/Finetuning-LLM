SELECT p.category, COUNT(DISTINCT oi.unit_price) AS unique_products, COUNT(oi.unit_price) AS unit_price_count
FROM order_items oi
RIGHT JOIN products p ON p.id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.id
WHERE oi.product_id IS NOT NULL
GROUP BY p.category;
