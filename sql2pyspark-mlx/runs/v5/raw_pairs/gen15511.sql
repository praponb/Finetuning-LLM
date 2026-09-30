SELECT city, COUNT(credit_limit) AS credit_limit_count
FROM customers
GROUP BY city
HAVING COUNT(credit_limit) >= 4
