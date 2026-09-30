SELECT a.account_type, a.status, c.segment
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.account_type <> 'LOAN'
ORDER BY segment;
