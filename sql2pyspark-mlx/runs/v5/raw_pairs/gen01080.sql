SELECT city, country, COUNT(*) AS total_count
FROM suppliers
WHERE city IS NOT NULL AND city IN ('Tokyo', 'New York', 'Chiang Mai')
GROUP BY city, country
HAVING SUM(CASE WHEN country = 'UK' THEN 1 ELSE 0 END) >= 62
ORDER BY city, country
