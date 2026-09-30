SELECT
    id,
    method,
    paid_at,
    amount,
    LEAD(amount, 1, 0) OVER (PARTITION BY method ORDER BY paid_at, id) AS next_amount
FROM payments
WHERE order_id IN (10, 13, 31, 40)
ORDER BY 1;
