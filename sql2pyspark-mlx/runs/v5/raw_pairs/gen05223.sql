SELECT
    id,
    status,
    method,
    ROUND(amount * 100, 2) AS amount_scaled
FROM payments
WHERE status IN ('REFUNDED', 'FAILED', 'SUCCESS') AND (order_id = 33 OR amount IS NOT NULL);
