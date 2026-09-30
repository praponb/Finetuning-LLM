SELECT
    id,
    method,
    paid_at,
    status,
    LEAD(amount) OVER (PARTITION BY order_id ORDER BY amount, id) AS next_amount,
    LAG(amount) OVER (PARTITION BY order_id ORDER BY amount, id) AS prev_amount,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS row_seq
FROM payments
WHERE status = 'SUCCESS';
