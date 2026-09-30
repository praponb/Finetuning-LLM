SELECT *, CAST(amount AS INT) amount_int
FROM payments
WHERE order_id IS NULL AND method != 'card' AND paid_at >= '2025-01-11'
ORDER BY amount;
