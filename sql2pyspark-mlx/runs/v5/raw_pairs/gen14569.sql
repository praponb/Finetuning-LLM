SELECT start_date
FROM subscriptions
UNION
SELECT shipped_date AS start_date
FROM shipments
WHERE cost <= 39;
