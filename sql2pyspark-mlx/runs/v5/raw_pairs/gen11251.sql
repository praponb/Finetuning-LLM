SELECT i.id, i.last_restock
FROM inventory i
WHERE YEAR(i.last_restock) >= 2024 AND i.store_id IN (SELECT id FROM stores WHERE sqft BETWEEN 5493 AND 7521)
ORDER BY last_restock, i.id;
