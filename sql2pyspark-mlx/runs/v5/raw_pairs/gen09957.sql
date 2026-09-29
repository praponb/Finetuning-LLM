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
    AVG(duration_sec) AS total,
    COUNT(1) AS item_count
    FOR page IN ('/checkout' AS checkout, '/search' AS search)
)
