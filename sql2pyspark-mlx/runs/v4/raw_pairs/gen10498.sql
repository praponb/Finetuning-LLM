SELECT YEAR(signup_date) AS signup_year, MAX(age) - MIN(age) AS age_range, MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range
FROM customers
GROUP BY YEAR(signup_date)
