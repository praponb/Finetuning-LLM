SELECT a.id, a.customer_id FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE segment IS NOT NULL) ORDER BY a.id DESC, customer_id DESC
