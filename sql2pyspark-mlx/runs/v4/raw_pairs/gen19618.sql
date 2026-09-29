SELECT a.id, a.account_type, a.customer_id
FROM accounts a
WHERE a.customer_id IN (SELECT id FROM customers WHERE segment = 'Corporate');
