SELECT delivered_date AS event_date, 'shipment' AS source
FROM shipments
UNION ALL
SELECT order_date AS event_date, 'order' AS source
FROM orders
