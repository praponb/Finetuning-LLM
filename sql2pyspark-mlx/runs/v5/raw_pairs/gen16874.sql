SELECT s.id, s.country, s.city FROM suppliers s WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id) ORDER BY city ASC, id ASC LIMIT 10;
