SELECT a.balance, c.name, c.credit_limit
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.customer_id = 30 AND c.name IS NULL;
