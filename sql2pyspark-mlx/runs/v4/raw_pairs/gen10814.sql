SELECT
    id,
    method,
    paid_at,
    amount,
    amount - LAG(amount, 1) OVER (PARTITION BY method ORDER BY paid_at, id) AS amount_change
FROM payments
WHERE TO_DATE(paid_at) < '2025-08-07'
ORDER BY amount DESC
