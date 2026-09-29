SELECT a.id, a.customer_id, a.status FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE LENGTH(email) < 6);
