SELECT id, price
FROM products
WHERE price >= (SELECT AVG(price) FROM products);
