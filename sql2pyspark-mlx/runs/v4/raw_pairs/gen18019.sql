SELECT s.city, p.product_name, p.stock_qty, p.supplier_id FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id;
