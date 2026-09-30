SELECT *
FROM (
    SELECT
        session_id,
        page,
        duration_sec
    FROM web_events
) src
PIVOT (
    MAX(duration_sec) AS value_sum,
    COUNT(1) AS cnt
    FOR page IN ('/checkout' AS checkout, '/product' AS product)
);
