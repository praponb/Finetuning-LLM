SELECT status
FROM shipments
WHERE shipped_date >= '2025-05-17'
INTERSECT
SELECT status
FROM subscriptions
ORDER BY status;
