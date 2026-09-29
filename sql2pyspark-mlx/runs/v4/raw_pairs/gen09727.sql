SELECT end_date event_date
FROM subscriptions
UNION
SELECT delivered_date event_date
FROM shipments
WHERE order_id IS NULL
ORDER BY event_date;
