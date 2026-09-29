SELECT carrier, status
FROM shipments
INTERSECT
SELECT carrier, status
FROM shipments
WHERE shipped_date >= '2024-01-01'
