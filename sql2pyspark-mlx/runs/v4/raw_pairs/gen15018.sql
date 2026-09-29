SELECT status
FROM orders
UNION
SELECT status
FROM payments
WHERE status NOT IN ('SUCCESS', 'FAILED');
