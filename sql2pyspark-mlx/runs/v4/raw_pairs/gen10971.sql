SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, SUM(credit_limit) AS total_credit_limit, COUNT(DISTINCT age) AS unique_citys
FROM customers
WHERE name IS NOT NULL
GROUP BY signup_month
HAVING unique_citys >= 1
ORDER BY signup_month
