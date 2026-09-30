SELECT
    id,
    order_id,
    method,
    amount,
    status,
    RANK() OVER (PARTITION BY method ORDER BY amount DESC NULLS FIRST) AS amount_rank,
    LEAD(amount, 2) OVER (ORDER BY amount, id) AS next_amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY order_id, status), 4) AS amount_share,
    AVG(amount) OVER w_ordered AS amount_avg_6,
    NTILE(4) OVER (ORDER BY amount DESC, id) AS amount_bucket
FROM payments
WHERE status = 'FAILED'
WINDOW w_ordered AS (PARTITION BY method ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
