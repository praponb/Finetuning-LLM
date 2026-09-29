SELECT
    id,
    paid_at,
    amount,
    LAG(amount, 2) OVER (ORDER BY paid_at, id) AS prev_amount
FROM payments;
