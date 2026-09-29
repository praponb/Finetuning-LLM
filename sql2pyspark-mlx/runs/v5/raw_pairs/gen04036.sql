SELECT a.id, a.balance, a.customer_id FROM accounts a WHERE a.status NOT IN ('CLOSED', 'OPEN') AND a.customer_id NOT IN (SELECT id FROM customers WHERE age >= 60) ORDER BY id;
