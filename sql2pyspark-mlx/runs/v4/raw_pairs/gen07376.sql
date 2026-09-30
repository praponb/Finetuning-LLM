SELECT id, price, stock_qty, brand FROM products WHERE price > (SELECT MAX(price) FROM products);
