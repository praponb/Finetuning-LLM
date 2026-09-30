SELECT * FROM accounts WHERE (customer_id IN (3, 14, 22) AND customer_id IS NULL) OR customer_id = 27;
