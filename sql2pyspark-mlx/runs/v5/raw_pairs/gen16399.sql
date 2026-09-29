WITH session_summary AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    WHERE event_type NOT IN ('click', 'search', 'add_to_cart')
    GROUP BY session_id
)
SELECT s.id, s.device, ss.stddev_duration_sec
FROM sessions s
LEFT JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.stddev_duration_sec >= 56
ORDER BY s.id DESC
