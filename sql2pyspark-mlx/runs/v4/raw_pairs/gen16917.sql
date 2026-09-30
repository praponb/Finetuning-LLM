SELECT status, 'order' source
FROM orders
WHERE shipping_fee BETWEEN 31 AND 39
UNION ALL
SELECT status, 'shipment' source
FROM shipments
ORDER BY source;
