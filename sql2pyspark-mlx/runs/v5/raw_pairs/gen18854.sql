SELECT city, ROUND(rating * 0.9, 2) AS adjusted_rating
FROM suppliers
WHERE city IN ('Bangkok', 'New York', 'Tokyo')
ORDER BY city;
