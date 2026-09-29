SELECT p.brand, COUNT(i.quantity) AS quantity_count, MAX(i.last_restock) AS last_last_restock
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
GROUP BY p.brand
ORDER BY brand;
