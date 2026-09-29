SELECT p.supplier_id, COUNT(DISTINCT i.quantity) AS unique_stores
FROM inventory i
JOIN products p ON i.product_id = p.id
WHERE i.product_id = 9
GROUP BY p.supplier_id
ORDER BY unique_stores DESC, p.supplier_id DESC;
