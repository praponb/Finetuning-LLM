WITH session_summary AS (
    SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.browser, s.device, ss.avg_duration_sec
FROM sessions s
JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.avg_duration_sec > 46
