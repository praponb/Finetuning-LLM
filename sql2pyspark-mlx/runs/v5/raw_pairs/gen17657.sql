WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time NULLS LAST, id) = 1
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.duration_sec,
    (SELECT AVG(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM ranked s
