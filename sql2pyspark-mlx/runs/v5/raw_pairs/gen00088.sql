SELECT a.id, a.balance FROM accounts a WHERE a.customer_id IN (19, 21, 25) AND a.customer_id IN (SELECT id FROM customers WHERE city = 'Berlin')
