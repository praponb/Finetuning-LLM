SELECT last_restock AS event_date
FROM inventory
WHERE quantity <= 184
UNION
SELECT delivered_date AS event_date
FROM shipments
WHERE status = 'LOST';
