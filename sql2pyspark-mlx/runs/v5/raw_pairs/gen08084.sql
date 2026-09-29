SELECT id, stock_qty FROM products WHERE stock_qty >= (SELECT AVG(stock_qty) FROM products) ORDER BY stock_qty, id LIMIT 3
