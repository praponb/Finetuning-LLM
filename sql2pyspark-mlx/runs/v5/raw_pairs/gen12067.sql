SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    RANK() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS FIRST) AS amount_rank,
    ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS row_seq
FROM payments
WHERE method <> 'cash'
