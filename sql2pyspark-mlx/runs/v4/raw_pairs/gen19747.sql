SELECT id, price, cost
FROM products
WHERE price > (SELECT AVG(price) FROM products WHERE category IN ('Home', 'Sports'))
