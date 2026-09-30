SELECT status
FROM accounts
UNION
SELECT status
FROM shipments
WHERE status IN ('IN_TRANSIT', 'LOST', 'DELIVERED');
