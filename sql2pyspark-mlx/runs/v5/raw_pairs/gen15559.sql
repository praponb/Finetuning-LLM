SELECT MONTH(signup_date) AS signup_month, SUM(credit_limit) AS total_credit_limit, SUM(SUM(credit_limit)) OVER (ORDER BY MONTH(signup_date)) AS cumulative_credit_limit
FROM customers
GROUP BY MONTH(signup_date);
