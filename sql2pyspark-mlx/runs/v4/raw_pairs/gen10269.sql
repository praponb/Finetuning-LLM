SELECT a.id, a.balance, a.status, a.opened_date FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE city = 'Singapore')
