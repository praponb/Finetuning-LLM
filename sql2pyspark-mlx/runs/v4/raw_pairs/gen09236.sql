SELECT * FROM (SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, country, age FROM customers)
PIVOT (SUM(age) FOR country IN ('SG', 'UK', 'TH'))
ORDER BY signup_month;
