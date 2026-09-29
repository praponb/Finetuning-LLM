SELECT status
FROM shipments
WHERE carrier = 'DHL'
UNION ALL
SELECT status
FROM subscriptions
WHERE customer_id = 22;
