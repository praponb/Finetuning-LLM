SELECT * FROM (SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, country, credit_limit FROM customers)
PIVOT (SUM(credit_limit) FOR country IN ('JP', 'UK'))
