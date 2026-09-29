SELECT category, SUM(price) AS total_price, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(price) DESC) AS rnk FROM products GROUP BY category;
