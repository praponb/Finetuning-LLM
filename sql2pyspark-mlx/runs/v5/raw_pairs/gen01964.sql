SELECT
    id,
    order_id,
    status,
    paid_at,
    amount,
    LAG(amount, 1) OVER (PARTITION BY order_id, status ORDER BY paid_at, id) AS prev_amount
FROM payments
ORDER BY id ASC, 5 DESC
