WITH with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
)
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
FROM with_gaps;
