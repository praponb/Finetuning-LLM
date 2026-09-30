WITH top_web_events AS (
    SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC) pos
    FROM web_events
)
SELECT id, session_id, event_time
FROM top_web_events
WHERE pos <= 5;
