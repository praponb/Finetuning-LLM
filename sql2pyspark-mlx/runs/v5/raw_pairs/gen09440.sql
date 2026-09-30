SELECT *
FROM (
    SELECT
        session_id,
        page,
        duration_sec
    FROM web_events
) src
PIVOT (
    AVG(duration_sec)
    FOR page IN ('/home' AS home, '/product' AS product, '/cart' AS cart)
);
