SELECT city, COUNT(age) AS age_count
FROM customers
GROUP BY city
HAVING age_count < 5
ORDER BY city DESC;
