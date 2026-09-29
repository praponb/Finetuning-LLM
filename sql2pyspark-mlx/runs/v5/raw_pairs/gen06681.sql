WITH session_summary AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, COUNT(*) AS row_count
    FROM web_events
    WHERE event_time < '2024-08-01'
    GROUP BY session_id
)
SELECT
    s.browser,
    s.device,
    ss.stddev_duration_sec,
    ss.row_count
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
ORDER BY browser;
