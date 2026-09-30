SELECT s.supplier_name, s.city, s.rating, p.stock_qty, p.brand FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id
