SELECT country, MONTH(signup_date) AS signup_month, MAX(signup_date) AS latest_signup_date FROM customers GROUP BY country, MONTH(signup_date) ORDER BY signup_month DESC, country ASC;
