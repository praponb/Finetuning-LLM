SELECT c.id, c.is_active, c.credit_limit FROM customers c WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id)
