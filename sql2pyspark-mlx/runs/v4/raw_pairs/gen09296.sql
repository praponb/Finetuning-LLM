SELECT status
FROM shipments
WHERE shipped_date > '2024-11-01'
UNION
SELECT status
FROM subscriptions
ORDER BY status;
