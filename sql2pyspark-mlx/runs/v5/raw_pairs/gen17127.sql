SELECT shipped_date AS event_date
FROM shipments
WHERE cost <= 57
INTERSECT
SELECT start_date AS event_date
FROM subscriptions
WHERE status = 'ACTIVE'
