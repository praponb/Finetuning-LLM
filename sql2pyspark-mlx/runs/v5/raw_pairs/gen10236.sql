WITH session_summary AS (
    SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT sessions.id, sessions.started_at, session_summary.avg_duration_sec
FROM sessions
LEFT JOIN session_summary ON sessions.id = session_summary.session_id
ORDER BY sessions.id;
