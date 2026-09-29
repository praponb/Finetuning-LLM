WITH session_stats AS (
    SELECT session_id, SUM(CASE WHEN event_type = 'page_view' THEN duration_sec ELSE 0 END) AS page_view_duration_sec, MAX(event_time) AS last_event_time
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.browser,
    s.started_at,
    ss.page_view_duration_sec,
    ss.last_event_time
FROM sessions s
INNER JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.page_view_duration_sec >= 46
ORDER BY ss.last_event_time DESC
