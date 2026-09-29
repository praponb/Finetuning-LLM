WITH top_web_events AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) AS rnk
    FROM web_events
    WHERE event_time >= '2023-11-01'
)
SELECT
    id,
    page,
    duration_sec,
    event_time
FROM top_web_events
WHERE rnk <= 2
ORDER BY event_time;
