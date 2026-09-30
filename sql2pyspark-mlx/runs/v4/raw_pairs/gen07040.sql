SELECT id, name, signup_date, email, CEIL(age) AS age_whole FROM customers WHERE country IN ('SG', 'JP')
