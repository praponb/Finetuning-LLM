SELECT id, session_id, duration_sec
FROM (
    SELECT
        id,
        session_id,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS pos
    FROM web_events
) AS ranked
WHERE pos <= 1;
