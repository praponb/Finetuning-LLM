SELECT launch_date, 'product' source FROM products UNION SELECT opened_date, 'store' source FROM stores ORDER BY source DESC
