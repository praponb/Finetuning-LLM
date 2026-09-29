SELECT
    id,
    status,
    method,
    paid_at
FROM payments
WHERE order_id IN (27, 32) AND status IN ('SUCCESS', 'REFUNDED');
