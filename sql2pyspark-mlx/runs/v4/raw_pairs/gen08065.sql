SELECT s.id, s.rating, s.supplier_name FROM suppliers s WHERE EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.price >= 500) ORDER BY supplier_name DESC
