SELECT a.id, a.account_type, a.status FROM accounts a WHERE a.customer_id IN (10, 11, 14) AND a.customer_id IN (SELECT id FROM customers WHERE name LIKE 'Em%');
