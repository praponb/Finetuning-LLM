SELECT *
FROM (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) AS pos
    FROM web_events
    WHERE session_id IN (6, 8)
) AS ranked
WHERE pos <= 1
ORDER BY duration_sec DESC;
