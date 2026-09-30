SELECT
    id,
    status,
    paid_at,
    amount,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY paid_at) AS paid_at_dense_rank,
    LEAD(amount, 1) OVER (PARTITION BY status ORDER BY paid_at, id) AS next_amount
FROM payments
