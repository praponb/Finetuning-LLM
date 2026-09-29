SELECT c.id, c.email FROM customers c WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id) ORDER BY id, email
