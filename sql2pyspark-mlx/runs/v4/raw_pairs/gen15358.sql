WITH prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
with_gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    prev_started_at,
    is_new_session,
    session_id
FROM islands
ORDER BY id DESC NULLS LAST, session_id DESC;
