SELECT status
FROM orders
WHERE discount > 0.28
UNION
SELECT status
FROM accounts
WHERE status = 'OPEN';
