SELECT
    id,
    session_id,
    duration_sec,
    event_type,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS rn,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS duration_sec_row
FROM web_events
ORDER BY duration_sec_row DESC
