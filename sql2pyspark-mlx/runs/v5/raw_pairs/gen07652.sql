WITH with_prev AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
flagged AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM flagged
