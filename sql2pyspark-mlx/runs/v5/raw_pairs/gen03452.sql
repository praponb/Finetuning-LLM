SELECT category, price_count FROM (SELECT category, COUNT(price) AS price_count FROM products GROUP BY category) AS sub WHERE price_count < 10 ORDER BY category LIMIT 5
