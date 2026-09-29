SELECT is_active, MAX(signup_date) AS latest_signup_date, COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count FROM customers WHERE segment = 'Corporate' GROUP BY 1 ORDER BY 2
