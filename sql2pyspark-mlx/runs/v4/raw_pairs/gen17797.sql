SELECT status
FROM payments
WHERE TO_DATE(paid_at) >= '2024-06-01'
UNION
SELECT status
FROM shipments
ORDER BY status
