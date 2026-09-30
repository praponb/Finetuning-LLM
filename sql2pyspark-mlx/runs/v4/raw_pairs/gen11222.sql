SELECT id, carrier
FROM shipments
INTERSECT ALL
SELECT id, carrier
FROM shipments
WHERE carrier IN ('UPS', 'DHL', 'Flash')
