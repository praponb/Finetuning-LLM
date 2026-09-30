SELECT
    id,
    method,
    paid_at,
    amount,
    LEAD(amount, 1) OVER (PARTITION BY method ORDER BY paid_at, id) AS next_amount
FROM payments
WHERE method IN ('cash', 'paypal', 'promptpay')
