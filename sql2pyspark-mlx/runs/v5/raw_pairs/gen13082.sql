SELECT segment, ROUND(AVG(age), 2) AS avg_age
FROM customers
GROUP BY segment
HAVING COUNT(age) > 3
