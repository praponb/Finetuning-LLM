SELECT
    id,
    amount,
    status,
    CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END AS is_refunded
FROM payments
