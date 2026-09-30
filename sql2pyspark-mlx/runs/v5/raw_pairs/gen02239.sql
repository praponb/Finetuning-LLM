WITH Numbered AS (
    SELECT
        id,
        event_type,
        event_time,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rn
    FROM web_events
)
SELECT *
FROM Numbered
WHERE rn <= 3
