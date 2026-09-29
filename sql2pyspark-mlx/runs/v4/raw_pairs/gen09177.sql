SELECT city, COUNT(DISTINCT credit_limit) AS unique_segments
FROM customers
WHERE credit_limit BETWEEN 4700 AND 13700 AND credit_limit <= 13200 AND NOT is_active
GROUP BY city
ORDER BY city
