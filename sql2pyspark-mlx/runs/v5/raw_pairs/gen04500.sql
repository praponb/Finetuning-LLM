SELECT last_restock
FROM inventory
UNION
SELECT shipped_date
FROM shipments
WHERE shipped_date BETWEEN '2023-03-30' AND '2024-02-01'
