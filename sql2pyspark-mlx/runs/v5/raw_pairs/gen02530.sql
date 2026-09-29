SELECT QUARTER(signup_date) signup_quarter, MONTH(signup_date) signup_month, COUNT(age) age_count FROM customers GROUP BY QUARTER(signup_date), MONTH(signup_date) ORDER BY signup_month DESC
