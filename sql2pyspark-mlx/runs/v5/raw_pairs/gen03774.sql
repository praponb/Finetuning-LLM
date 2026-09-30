SELECT shipped_date
FROM shipments
WHERE cost BETWEEN 28 AND 37
UNION
SELECT shipped_date
FROM shipments
WHERE cost <= 53;
