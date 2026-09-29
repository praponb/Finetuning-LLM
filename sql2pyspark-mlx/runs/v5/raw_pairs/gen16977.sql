WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
)
-- Running sum of the flag = session number
SELECT
    id,
    order_id,
    method,
    paid_at,
    status,
    SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM flagged
