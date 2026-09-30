WITH numbered AS (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        DENSE_RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC) pos
    FROM web_events
)
SELECT
    id,
    session_id,
    event_time,
    duration_sec
FROM numbered
WHERE pos <= 1
ORDER BY id, session_id
