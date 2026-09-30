SELECT id, segment, credit_limit, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) rn FROM customers WHERE MONTH(signup_date) IN (2, 3, 4);
