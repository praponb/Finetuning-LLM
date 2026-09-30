WITH session_summary AS (
    SELECT session_id, SUM(duration_sec) total_duration_sec, COUNT(duration_sec) duration_sec_count
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.device,
    s.started_at,
    ss.total_duration_sec,
    ss.duration_sec_count
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.duration_sec_count > 8
ORDER BY duration_sec_count DESC, id
LIMIT 10;
