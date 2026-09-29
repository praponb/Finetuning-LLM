SELECT id, stock_qty
FROM products
WHERE stock_qty > (SELECT AVG(stock_qty) FROM products)
ORDER BY id;
