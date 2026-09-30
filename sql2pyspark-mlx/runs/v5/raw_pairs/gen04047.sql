SELECT s.id, s.city, s.country, s.supplier_name FROM suppliers s WHERE s.rating <= 4.2 AND NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.price >= 780) ORDER BY id DESC
