SELECT status
FROM shipments
WHERE delivered_date BETWEEN '2023-07-01' AND '2024-03-30'
UNION
SELECT status
FROM shipments
WHERE carrier = 'Flash'
ORDER BY status;
