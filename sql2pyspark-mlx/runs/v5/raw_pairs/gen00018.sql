WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time
    FROM web_events
    WHERE event_type != 'click'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        session_id,
        page,
        duration_sec
    FROM cleaned
) src
PIVOT (
    SUM(duration_sec) AS amount,
    COUNT(1) AS item_count
    FOR page IN ('/search' AS search, '/cart' AS cart)
)
