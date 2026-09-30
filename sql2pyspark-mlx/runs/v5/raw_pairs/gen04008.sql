SELECT s.id, s.supplier_name, s.country
FROM suppliers s
WHERE s.city = 'New York' AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.supplier_id = 14)
