SELECT c.id, c.city FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.balance <= 44000) ORDER BY city;
