SELECT s.supplier_name, s.rating, s.city, p.stock_qty, p.price FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id
