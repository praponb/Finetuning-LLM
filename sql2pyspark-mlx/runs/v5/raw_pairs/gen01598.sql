SELECT id, status, method
FROM payments
WHERE amount < 1560
UNION
SELECT id, status, method
FROM payments
WHERE paid_at >= '2023-09-20'
