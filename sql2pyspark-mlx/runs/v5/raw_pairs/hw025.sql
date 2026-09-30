SELECT category, MIN(price) AS min_price, MAX(price) AS max_price FROM products GROUP BY category;
