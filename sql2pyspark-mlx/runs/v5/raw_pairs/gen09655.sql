WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END is_refunded
    FROM payments
    WHERE amount > 1460
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM valid
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Grouped metrics
SELECT
    CASE WHEN session_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
    CASE WHEN is_refunded >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_refunded_tier,
    SIZE(COLLECT_SET(method)) AS num_methods,
    COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows,
    GROUPING_ID() AS grouping_level
FROM sessionized
GROUP BY GROUPING SETS ((CASE WHEN session_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN is_refunded >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN session_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
