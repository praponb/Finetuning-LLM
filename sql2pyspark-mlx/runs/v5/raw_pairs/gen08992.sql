SELECT 'payment' AS record_type, status
FROM payments
UNION ALL
SELECT 'shipment' AS record_type, status
FROM shipments
