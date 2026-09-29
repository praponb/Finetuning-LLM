SELECT YEAR(signup_date) AS signup_year, COUNT(DISTINCT credit_limit) AS unique_citys
FROM customers
WHERE email IS NOT NULL AND email IS NOT NULL
GROUP BY YEAR(signup_date);
