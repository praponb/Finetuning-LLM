SELECT status
FROM shipments
UNION ALL
SELECT status
FROM subscriptions
WHERE status <> 'ACTIVE'
