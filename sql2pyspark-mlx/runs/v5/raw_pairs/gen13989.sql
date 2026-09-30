SELECT *
FROM (
    SELECT
        id,
        session_id,
        duration_sec,
        page,
        DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) pos
    FROM web_events
) ranked
WHERE pos <= 3
ORDER BY duration_sec DESC, session_id DESC
