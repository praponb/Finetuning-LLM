SELECT order_date, 'order' AS source
FROM orders
UNION
SELECT shipped_date, 'shipment' AS source
FROM shipments
WHERE cost BETWEEN 36 AND 95
