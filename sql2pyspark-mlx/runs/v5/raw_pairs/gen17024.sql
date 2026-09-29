SELECT id, price, cost
FROM products
WHERE price >= (SELECT MAX(price) FROM products)
ORDER BY cost DESC
