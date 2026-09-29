SELECT s.id, s.rating
FROM suppliers s
WHERE s.supplier_name LIKE '%Pacific%' AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id IS NULL);
