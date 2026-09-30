SELECT
    id,
    order_id,
    paid_at,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY paid_at DESC) AS paid_at_dense_rank
FROM payments
WHERE status = 'SUCCESS'
