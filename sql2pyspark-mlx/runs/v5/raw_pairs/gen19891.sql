SELECT id, order_id
FROM payments
WHERE amount BETWEEN 1210 AND 1340
UNION
SELECT id, order_id
FROM payments
WHERE paid_at >= '2024-10-01'
ORDER BY id;
