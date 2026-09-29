SELECT 'order' AS source, status
FROM orders
WHERE shipping_fee <= 32
UNION ALL
SELECT 'subscription' AS source, status
FROM subscriptions
WHERE monthly_fee IS NOT NULL
