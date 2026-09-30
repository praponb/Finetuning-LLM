SELECT id, supplier_id, stock_qty, MIN(stock_qty) OVER (PARTITION BY supplier_id) AS supplier_id_min_stock_qty FROM products;
