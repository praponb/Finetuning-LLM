SELECT a.id, a.balance
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit <= 8800);
