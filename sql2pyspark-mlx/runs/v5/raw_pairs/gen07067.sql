WITH session_stats AS (
    SELECT session_id, SUM(duration_sec) AS total_duration_sec, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.country,
    s.browser,
    ss.total_duration_sec,
    ss.max_duration_sec
FROM sessions s
JOIN session_stats ss ON s.id = ss.session_id
WHERE ss.total_duration_sec > 1500
