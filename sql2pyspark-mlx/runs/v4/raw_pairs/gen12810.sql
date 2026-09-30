SELECT city, COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count FROM customers WHERE LENGTH(email) >= 10 GROUP BY city HAVING AVG(credit_limit) > 5700 ORDER BY tokyo_count, city LIMIT 20
