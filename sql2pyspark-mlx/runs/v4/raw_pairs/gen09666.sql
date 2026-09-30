SELECT last_restock
FROM inventory
UNION
SELECT shipped_date
FROM shipments
ORDER BY last_restock
