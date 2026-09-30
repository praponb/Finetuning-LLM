SELECT last_restock
FROM inventory
WHERE quantity BETWEEN 150 AND 227
INTERSECT
SELECT shipped_date AS last_restock
FROM shipments;
