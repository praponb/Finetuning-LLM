WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) AS prev_paid_at
    FROM payments
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
)
-- Running sum of the flag = session number
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM with_gaps
