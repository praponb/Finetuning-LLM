SELECT QUARTER(signup_date) AS signup_quarter, city, MAX(signup_date) AS latest_signup_date FROM customers WHERE is_active = TRUE GROUP BY signup_quarter, city ORDER BY latest_signup_date
