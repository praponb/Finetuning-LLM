SELECT 'shipment' AS record_type, delivered_date AS event_date
FROM shipments
UNION
SELECT 'subscription' AS record_type, start_date AS event_date
FROM subscriptions
WHERE start_date > '2024-11-01'
ORDER BY record_type
