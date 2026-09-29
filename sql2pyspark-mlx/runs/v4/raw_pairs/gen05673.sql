SELECT s.region, MIN(i.last_restock) AS min_last_restock, COUNT(DISTINCT i.quantity) AS unique_products
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON p.id = i.product_id
GROUP BY s.region
ORDER BY 1
LIMIT 10;
