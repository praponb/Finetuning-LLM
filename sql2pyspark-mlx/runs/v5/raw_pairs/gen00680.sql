SELECT s.rating, s.supplier_name, p.launch_date, p.category, p.brand FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id;
