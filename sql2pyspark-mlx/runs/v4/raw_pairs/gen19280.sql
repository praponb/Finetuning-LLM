SELECT p.category, AVG(oi.unit_price) AS avg_unit_price, COUNT(DISTINCT oi.unit_price) AS unique_products
FROM order_items oi
LEFT JOIN orders o ON o.id = oi.order_id
FULL OUTER JOIN products p ON oi.product_id = p.id
WHERE p.category NOT IN ('Home', 'Books', 'Fashion')
GROUP BY p.category;
