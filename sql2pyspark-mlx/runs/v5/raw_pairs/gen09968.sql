SELECT cost, 'shipment' AS record_type
FROM shipments
UNION ALL
SELECT amount, 'payment' AS record_type
FROM payments
