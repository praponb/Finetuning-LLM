WITH with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.duration_sec,
    s.session_no,
    (SELECT MAX(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS max_web_event_duration_sec
FROM sessionized s
ORDER BY session_id DESC NULLS FIRST
