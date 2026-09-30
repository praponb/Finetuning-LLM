SELECT p.supplier_id, COUNT(DISTINCT i.quantity) AS unique_products
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
LEFT OUTER JOIN stores s ON i.store_id = s.id
WHERE i.store_id IN (1, 6)
GROUP BY p.supplier_id;
