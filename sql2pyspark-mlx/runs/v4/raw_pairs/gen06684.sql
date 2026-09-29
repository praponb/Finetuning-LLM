SELECT amount, 'payment' AS source
FROM payments
UNION
SELECT cost, 'shipment' AS source
FROM shipments;
