SELECT s.id, s.city, s.rating, s.supplier_name FROM suppliers s WHERE s.rating >= 1.9 AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id) ORDER BY city, id LIMIT 10
