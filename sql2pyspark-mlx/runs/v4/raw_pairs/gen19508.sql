SELECT status
FROM accounts
UNION ALL
SELECT status
FROM shipments
ORDER BY status;
