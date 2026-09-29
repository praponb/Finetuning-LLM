SELECT p.id, p.launch_date, p.supplier_id, s.supplier_name, s.rating, s.city FROM products p JOIN suppliers s ON p.supplier_id = s.id
