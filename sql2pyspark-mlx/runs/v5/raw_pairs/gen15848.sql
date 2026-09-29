SELECT status FROM orders WHERE discount < 0.36 UNION SELECT status FROM accounts WHERE customer_id IS NOT NULL;
