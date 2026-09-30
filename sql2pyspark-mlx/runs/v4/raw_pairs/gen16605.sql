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
    SUM(duration_sec) AS value_sum,
    COUNT(1) AS cnt
    FOR page IN ('/cart' AS cart, '/product' AS product, '/search' AS search)
);
