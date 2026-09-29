SELECT p.id, p.product_name, COUNT(i.id) AS inventory_count
FROM products p
INNER JOIN inventory i ON p.id = i.product_id
WHERE p.category = 'Books'
GROUP BY p.id, p.product_name
ORDER BY inventory_count DESC;
