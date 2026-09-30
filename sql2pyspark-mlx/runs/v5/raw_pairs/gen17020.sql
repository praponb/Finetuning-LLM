SELECT
    id,
    event_type,
    session_id,
    duration_sec,
    NTILE(4) OVER (PARTITION BY event_type, session_id ORDER BY duration_sec DESC, id) AS duration_sec_ntile
FROM web_events;
