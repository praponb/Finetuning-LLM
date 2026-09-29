SELECT p.stock_qty, p.launch_date, s.country, s.city FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY launch_date;
