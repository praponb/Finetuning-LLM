SELECT p.price, p.stock_qty, s.supplier_name, s.country, s.city FROM products p INNER JOIN suppliers s ON s.id = p.supplier_id
