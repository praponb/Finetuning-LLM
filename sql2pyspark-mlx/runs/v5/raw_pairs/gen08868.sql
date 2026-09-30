WITH session_stats AS (
    SELECT session_id, MAX(event_time) last_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT s.customer_id, s.device, ss.last_event_time
FROM sessions s
LEFT OUTER JOIN session_stats ss ON s.id = ss.session_id
ORDER BY ss.last_event_time;
