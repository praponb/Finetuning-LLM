SELECT status, 'payment' source
FROM payments
WHERE method = 'cash'
UNION
SELECT status, 'shipment' source
FROM shipments
ORDER BY status;
