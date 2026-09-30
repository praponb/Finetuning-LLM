SELECT
    id,
    event_type,
    event_time,
    session_id
FROM (
    SELECT
        id,
        event_type,
        event_time,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rn
    FROM web_events
    WHERE duration_sec < 245
) AS ranked
WHERE rn <= 5
