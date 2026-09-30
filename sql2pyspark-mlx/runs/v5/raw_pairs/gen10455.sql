SELECT id, launch_date, stock_qty, supplier_id FROM products WHERE supplier_id = 7 AND supplier_id IN (4, 30) ORDER BY id, supplier_id DESC
