WITH session_stats AS (
    SELECT session_id, COUNT(*) AS cnt, SUM(duration_sec) AS total_duration_sec
    FROM web_events
    WHERE session_id IN (4, 25, 26)
    GROUP BY session_id
)
SELECT
    sessions.started_at,
    sessions.device,
    session_stats.cnt,
    session_stats.total_duration_sec
FROM sessions
INNER JOIN session_stats ON sessions.id = session_stats.session_id
ORDER BY device;
