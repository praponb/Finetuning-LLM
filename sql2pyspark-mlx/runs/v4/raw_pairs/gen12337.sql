WITH top_web_events AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS rn
    FROM web_events
    WHERE session_id IN (15, 24)
)
SELECT
    id,
    page,
    event_time,
    session_id
FROM top_web_events
WHERE rn = 1
ORDER BY session_id DESC, event_time
