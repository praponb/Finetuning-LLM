SELECT a.id, a.account_type, a.status, a.customer_id FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE age IS NOT NULL) ORDER BY a.status, customer_id DESC;
