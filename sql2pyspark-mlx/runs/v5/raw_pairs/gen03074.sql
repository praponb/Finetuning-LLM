SELECT s.country, COUNT(*) AS order_item_count, COUNT(DISTINCT oi.quantity) AS unique_products
FROM order_items oi
RIGHT JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
ORDER BY order_item_count DESC;
