SELECT *, CAST(is_active AS INT) AS is_active_flag, CEIL(credit_limit) AS credit_limit_whole FROM customers WHERE city <> 'New York' AND city = 'Bangkok' AND credit_limit >= 9000;
