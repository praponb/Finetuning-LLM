SELECT
    id,
    status,
    method,
    CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END AS is_refunded,
    ROUND(amount * 0.85, 2) AS amount_adjusted
FROM payments
WHERE status IN ('SUCCESS', 'FAILED', 'REFUNDED')
ORDER BY status
