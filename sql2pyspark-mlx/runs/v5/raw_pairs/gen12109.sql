SELECT city, SUM(sqft) AS total_sqft, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(sqft) DESC) AS rnk FROM stores WHERE city = 'Singapore' GROUP BY city ORDER BY 1, cnt;
