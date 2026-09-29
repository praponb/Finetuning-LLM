SELECT
    id,
    order_id,
    status,
    paid_at,
    amount,
    SUM(amount) OVER (PARTITION BY order_id, status ORDER BY paid_at, id) AS running_amount
FROM payments
