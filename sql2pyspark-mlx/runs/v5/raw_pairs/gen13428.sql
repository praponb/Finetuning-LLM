SELECT id, customer_id, balance, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS rn FROM accounts WHERE customer_id IN (22, 39)
