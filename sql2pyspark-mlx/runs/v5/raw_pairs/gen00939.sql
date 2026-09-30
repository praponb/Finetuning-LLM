SELECT *
FROM (
    SELECT
        event_type,
        session_id,
        page,
        duration_sec
    FROM web_events
) src
PIVOT (
    MAX(duration_sec) AS amount,
    COUNT(1) AS n
    FOR page IN ('/search' AS search, '/product' AS product)
);
