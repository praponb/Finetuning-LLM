SELECT id, stock_qty FROM products WHERE stock_qty = (SELECT MIN(stock_qty) FROM products WHERE brand IS NULL) ORDER BY stock_qty
