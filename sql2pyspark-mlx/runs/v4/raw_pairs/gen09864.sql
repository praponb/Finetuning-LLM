WITH recent_web_events AS (
    SELECT *
    FROM web_events
    WHERE session_id = 21 AND page IS NOT NULL
),
summary AS (
    SELECT page, SUM(CASE WHEN page = '/search' THEN duration_sec ELSE 0 END) AS search_duration_sec
    FROM recent_web_events
    GROUP BY page
)
SELECT page, search_duration_sec
FROM summary
WHERE search_duration_sec > 27
