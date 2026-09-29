SELECT id, credit_limit, is_active, name, CEIL(credit_limit) AS credit_limit_whole, CONCAT(segment, ' ', email) AS segment_email FROM customers WHERE MONTH(signup_date) IN (1, 6, 10);
