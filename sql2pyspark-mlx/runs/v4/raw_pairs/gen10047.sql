SELECT city, SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END) AS new_york_count
FROM suppliers
GROUP BY city
HAVING SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END) > 42
