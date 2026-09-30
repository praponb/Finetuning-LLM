SELECT
    id,
    method,
    paid_at,
    amount,
    SUM(amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
FROM payments
WHERE method IN ('paypal', 'promptpay', 'bank_transfer')
ORDER BY 4
