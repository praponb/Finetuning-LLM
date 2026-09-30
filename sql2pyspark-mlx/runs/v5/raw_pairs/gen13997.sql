SELECT *
FROM (
    SELECT
        event_type,
        page,
        duration_sec
    FROM web_events
) src
PIVOT (
    MAX(duration_sec)
    FOR page IN ('/product' AS product, '/search' AS search)
)
