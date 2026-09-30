SELECT s.id, s.city, s.supplier_name FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND MONTH(p.launch_date) IN (4, 5, 10))
