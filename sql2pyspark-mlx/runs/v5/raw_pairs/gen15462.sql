WITH top_web_events AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS rnk
    FROM web_events
)
SELECT
    id,
    event_type,
    event_time,
    page
FROM top_web_events
WHERE rnk <= 1;
