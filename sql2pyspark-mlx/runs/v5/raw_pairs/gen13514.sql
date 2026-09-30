SELECT p.id, p.stock_qty, p.category, p.launch_date FROM products p WHERE p.supplier_id IN (SELECT id FROM suppliers WHERE rating > 2.2) ORDER BY stock_qty DESC
