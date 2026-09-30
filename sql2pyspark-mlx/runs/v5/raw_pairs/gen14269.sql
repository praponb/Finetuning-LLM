WITH top_web_events AS (
    SELECT
        id,
        event_type,
        event_time,
        page,
        RANK() OVER (PARTITION BY event_type ORDER BY event_time DESC) AS rnk
    FROM web_events
    WHERE page NOT IN ('/search', '/cart')
)
SELECT
    id,
    event_type,
    event_time,
    page
FROM top_web_events
WHERE rnk <= 3
ORDER BY event_time, page DESC
