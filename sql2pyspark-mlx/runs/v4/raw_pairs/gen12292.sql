SELECT launch_date, product_name, FLOOR(stock_qty) AS stock_qty_whole, SIZE(tags) AS tags_count FROM products WHERE cost <= 430
