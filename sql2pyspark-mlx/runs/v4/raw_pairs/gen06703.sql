WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
)
-- Running sum of the flag = session number
SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM flagged
