SELECT status
FROM accounts
WHERE status IN ('FROZEN', 'CLOSED', 'OPEN')
UNION ALL
SELECT status
FROM shipments
ORDER BY status DESC
