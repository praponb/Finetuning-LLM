SELECT status
FROM payments
WHERE paid_at < '2023-06-01'
UNION
SELECT status
FROM shipments;
