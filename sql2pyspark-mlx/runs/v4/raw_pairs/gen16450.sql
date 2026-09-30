SELECT a.id, a.account_type, a.balance, a.status FROM accounts a WHERE a.balance >= 71000 AND a.customer_id NOT IN (SELECT id FROM customers WHERE name LIKE 'C%');
