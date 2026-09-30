SELECT delivered_date AS event_date
FROM shipments
UNION
SELECT last_restock AS event_date
FROM inventory;
