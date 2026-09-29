SELECT 'payment' AS source, status
FROM payments
UNION ALL
SELECT 'shipment' AS source, status
FROM shipments
WHERE carrier = 'DHL'
ORDER BY status
