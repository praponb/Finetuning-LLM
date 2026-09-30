SELECT 'order' AS kind, amount FROM orders WHERE amount < 1510 UNION ALL SELECT 'shipment' AS kind, cost FROM shipments
