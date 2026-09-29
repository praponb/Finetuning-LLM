SELECT *
FROM (
    SELECT
        session_id,
        page,
        duration_sec
    FROM web_events
) src
PIVOT (
    SUM(duration_sec) AS amount,
    COUNT(1) AS cnt
    FOR page IN ('/search' AS search, '/cart' AS cart)
)
