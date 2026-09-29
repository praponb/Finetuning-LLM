SELECT
    id,
    order_id,
    method,
    status,
    LEAD(amount, 2) OVER w_hist AS next_amount,
    ROW_NUMBER() OVER w_hist AS event_seq,
    SUM(amount) OVER (PARTITION BY order_id, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
    NTILE(10) OVER (PARTITION BY status, method ORDER BY amount DESC, id) AS amount_bucket
FROM payments
WHERE status = 'FAILED'
WINDOW w_hist AS (ORDER BY amount, id)
