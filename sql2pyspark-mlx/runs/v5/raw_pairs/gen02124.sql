SELECT c.id, c.name, c.is_active, c.signup_date FROM customers c WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id);
