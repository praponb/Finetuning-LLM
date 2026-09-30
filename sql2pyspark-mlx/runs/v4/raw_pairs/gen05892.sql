SELECT a.id, a.status FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE credit_limit < 12700)
