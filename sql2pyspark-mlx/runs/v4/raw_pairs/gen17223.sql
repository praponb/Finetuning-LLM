SELECT s.id, s.country
FROM suppliers s
WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id IS NOT NULL)
