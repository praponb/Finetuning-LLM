WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
)
-- Running sum of the flag = session number
SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM flagged
