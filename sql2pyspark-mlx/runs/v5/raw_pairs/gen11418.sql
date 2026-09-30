SELECT city, ROUND(AVG(rating), 2) AS avg_rating, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count FROM suppliers GROUP BY city
