SELECT s.id, s.city, s.supplier_name
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products)
ORDER BY s.supplier_name ASC, 2 ASC
