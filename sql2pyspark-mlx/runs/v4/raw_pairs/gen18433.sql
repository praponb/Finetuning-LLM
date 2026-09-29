SELECT s.id, s.country, s.city
FROM suppliers s
WHERE s.city = 'New York' AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id)
