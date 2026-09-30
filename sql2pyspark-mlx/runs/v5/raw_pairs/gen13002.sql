SELECT id, carrier, order_id
FROM shipments
WHERE order_id = 16
UNION
SELECT id, carrier, order_id
FROM shipments
WHERE carrier IN ('DHL', 'FedEx');
