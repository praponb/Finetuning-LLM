SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    AVG(amount) OVER w_recent AS amount_avg_30d,
    LAG(amount, 1) OVER w_ordered AS prev_amount,
    RANK() OVER (ORDER BY amount NULLS LAST) AS amount_rank,
    RANK() OVER (ORDER BY amount NULLS FIRST) AS amount_rank_2
FROM payments
WINDOW w_recent AS (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY order_id ORDER BY paid_at, id)
