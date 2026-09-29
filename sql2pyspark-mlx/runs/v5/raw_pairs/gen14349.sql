SELECT last_restock
FROM inventory
EXCEPT
SELECT shipped_date
FROM shipments
WHERE delivered_date >= '2025-04-25'
