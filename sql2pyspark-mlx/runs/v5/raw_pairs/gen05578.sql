SELECT start_date event_date
FROM subscriptions
INTERSECT
SELECT shipped_date event_date
FROM shipments;
