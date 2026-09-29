SELECT a.customer_id, c.signup_date
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
WHERE a.status = 'OPEN';
