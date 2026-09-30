SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, SUM(age) AS total_age, DENSE_RANK() OVER (ORDER BY SUM(age) DESC) AS rnk FROM customers GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
