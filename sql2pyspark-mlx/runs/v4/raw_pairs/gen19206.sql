SELECT delivered_date AS event_date
FROM shipments
EXCEPT
SELECT start_date AS event_date
FROM subscriptions;
