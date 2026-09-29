SELECT id, order_id
FROM payments
WHERE order_id IS NULL
UNION ALL
SELECT id, order_id
FROM payments
WHERE status IN ('REFUNDED', 'SUCCESS');
