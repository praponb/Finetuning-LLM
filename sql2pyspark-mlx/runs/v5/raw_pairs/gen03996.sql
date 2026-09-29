SELECT 'order' AS kind, status
FROM orders
UNION ALL
SELECT 'shipment' AS kind, status
FROM shipments;
