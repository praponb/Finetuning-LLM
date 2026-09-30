SELECT id, name, email, credit_limit * age AS credit_limit_times_age, CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END AS is_consumer FROM customers ORDER BY is_consumer DESC
