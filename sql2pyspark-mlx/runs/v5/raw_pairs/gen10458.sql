SELECT s.id, s.supplier_name
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products)
