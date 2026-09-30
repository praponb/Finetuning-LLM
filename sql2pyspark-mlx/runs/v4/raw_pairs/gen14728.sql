SELECT p.brand, SUM(i.quantity) AS total_quantity, MIN(i.last_restock) AS first_last_restock
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
WHERE i.product_id IN (1, 4, 10)
GROUP BY p.brand
