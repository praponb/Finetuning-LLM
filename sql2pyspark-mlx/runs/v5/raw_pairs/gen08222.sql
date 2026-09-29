SELECT id, name, COALESCE(credit_limit, 0) AS credit_limit_filled FROM customers WHERE signup_date >= '2023-03-09'
