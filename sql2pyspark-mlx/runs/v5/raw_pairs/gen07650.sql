SELECT s.country, COUNT(DISTINCT i.quantity) AS unique_stores
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE i.store_id IN (2, 3, 4, 6)
GROUP BY s.country;
