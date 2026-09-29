SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    session_id,
    page,
    SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id) AS running_duration_sec,
    MIN(duration_sec) OVER (PARTITION BY session_id, page) AS session_id_min_duration_sec
FROM web_events
