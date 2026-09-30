SELECT product_name, tags, stock_qty * price AS stock_qty_times_price FROM products WHERE supplier_id IN (4, 6, 19, 32) AND supplier_id = 12
