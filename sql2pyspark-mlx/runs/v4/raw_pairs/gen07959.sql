SELECT stock_qty, brand FROM products WHERE stock_qty <> 315 UNION SELECT stock_qty, brand FROM products WHERE price BETWEEN 380 AND 450
