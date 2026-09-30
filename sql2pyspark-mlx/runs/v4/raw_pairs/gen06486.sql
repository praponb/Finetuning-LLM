WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        LEAD(duration_sec, 2) OVER w_running AS next_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY page, event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        NTILE(10) OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS duration_sec_bucket
    FROM web_events
    WINDOW w_running AS (PARTITION BY event_type, session_id ORDER BY event_time, id)
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.duration_sec,
    s.cumulative_duration_sec,
    s.duration_sec_bucket,
    (SELECT MAX(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS max_web_event_duration_sec
FROM metrics s
ORDER BY duration_sec DESC NULLS FIRST
