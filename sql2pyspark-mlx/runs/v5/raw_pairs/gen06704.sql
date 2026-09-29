SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, COUNT(DISTINCT age) AS unique_citys FROM customers GROUP BY signup_month
