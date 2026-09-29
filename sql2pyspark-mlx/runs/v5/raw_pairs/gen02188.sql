SELECT id, cost, stock_qty
FROM products
WHERE cost > (SELECT AVG(cost) FROM products);
