SELECT p.id, p.product_name, COUNT(i.id) AS inventory_count
FROM products p
LEFT JOIN inventory i ON p.id = i.product_id
WHERE i.product_id IN (1, 4, 9)
GROUP BY p.id, p.product_name
ORDER BY inventory_count DESC, product_name, id
LIMIT 1;
