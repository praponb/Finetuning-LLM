SELECT
    status,
    MIN(amount) AS min_amount,
    COUNT(amount) AS amount_count,
    AVG(amount) AS avg_amount
FROM payments
WHERE status IN ('FAILED', 'REFUNDED') AND paid_at >= '2023-11-03'
GROUP BY status;
