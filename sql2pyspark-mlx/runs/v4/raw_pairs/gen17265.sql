SELECT s.id, s.supplier_name, s.city
FROM suppliers s
WHERE s.supplier_name LIKE 'Si%' AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)
ORDER BY s.supplier_name;
