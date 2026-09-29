SELECT p.id, COUNT(i.id) AS inventory_count, AVG(i.quantity) AS avg_quantity
FROM products p
LEFT JOIN inventory i ON p.id = i.product_id
WHERE p.brand = 'Initech'
GROUP BY p.id
ORDER BY id DESC;
