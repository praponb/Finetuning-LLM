SELECT city, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit, AVG(age) AS avg_age
FROM customers
WHERE city = 'Berlin'
GROUP BY city
HAVING ROUND(STDDEV(credit_limit), 2) > 47
ORDER BY city ASC
LIMIT 3
