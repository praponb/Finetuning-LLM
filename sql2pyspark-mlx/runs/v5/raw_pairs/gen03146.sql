WITH ranked_web_events AS (
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS rnk
    FROM web_events
)
SELECT
    id,
    page,
    event_time,
    duration_sec
FROM ranked_web_events
WHERE rnk <= 3
ORDER BY 3
