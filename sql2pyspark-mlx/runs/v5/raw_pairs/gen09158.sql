SELECT id, supplier_id, category FROM products WHERE stock_qty < 370 UNION ALL SELECT id, supplier_id, category FROM products WHERE launch_date < '2023-09-19' ORDER BY id DESC
