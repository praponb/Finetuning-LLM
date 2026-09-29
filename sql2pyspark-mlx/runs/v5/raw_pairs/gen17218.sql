SELECT s.region, MIN(i.last_restock) AS first_last_restock, MAX(i.quantity) AS max_quantity
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
JOIN stores s ON i.store_id = s.id
WHERE i.quantity BETWEEN 167 AND 221
GROUP BY s.region
ORDER BY region
