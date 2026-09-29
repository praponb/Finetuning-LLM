SELECT amount, 'order' AS source
FROM orders
UNION
SELECT cost, 'shipment' AS source
FROM shipments
WHERE status = 'DELIVERED';
