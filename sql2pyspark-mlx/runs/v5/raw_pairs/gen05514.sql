SELECT city, SUM(CASE WHEN city = 'New York' THEN sqft ELSE 0 END) AS new_york_sqft
FROM stores
WHERE city NOT IN ('New York', 'Bangkok')
GROUP BY 1
HAVING SUM(CASE WHEN city = 'New York' THEN sqft ELSE 0 END) > 50
ORDER BY city ASC;
