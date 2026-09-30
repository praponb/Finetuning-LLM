SELECT 'subscription' AS source, end_date
FROM subscriptions
UNION ALL
SELECT 'shipment' AS source, shipped_date
FROM shipments;
