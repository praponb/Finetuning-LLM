SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, COUNT(credit_limit) AS credit_limit_count
FROM customers
GROUP BY DATE_TRUNC('MONTH', signup_date)
ORDER BY credit_limit_count DESC
