SELECT id, stock_qty, cost
FROM products
WHERE brand IN ('Globex', 'Stark') AND price < 760
ORDER BY id
