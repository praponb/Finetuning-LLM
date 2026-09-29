SELECT * FROM (SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, city, credit_limit FROM customers WHERE age != 48)
PIVOT (COUNT(credit_limit) FOR city IN ('Singapore', 'Tokyo', 'Bangkok'))
ORDER BY signup_month;
