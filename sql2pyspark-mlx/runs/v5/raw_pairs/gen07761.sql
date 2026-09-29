SELECT
    id,
    session_id,
    event_time,
    event_type
FROM (
    SELECT
        id,
        session_id,
        event_time,
        event_type,
        DENSE_RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC) AS row_num
    FROM web_events
) AS ranked
WHERE row_num <= 2
ORDER BY session_id DESC, event_type;
