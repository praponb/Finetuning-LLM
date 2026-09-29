SELECT p.id, p.stock_qty, s.country, s.supplier_name FROM products p LEFT JOIN suppliers s ON s.id = p.supplier_id WHERE p.supplier_id IN (11, 17, 26) ORDER BY id DESC
