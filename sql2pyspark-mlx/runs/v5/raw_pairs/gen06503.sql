WITH with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
with_gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    id,
    method,
    amount,
    status,
    prev_paid_at
FROM sessionized
ORDER BY amount DESC
