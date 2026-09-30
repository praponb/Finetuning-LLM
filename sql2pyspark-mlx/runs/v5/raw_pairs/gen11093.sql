WITH session_summary AS (
    SELECT session_id, MIN(duration_sec) min_duration_sec
    FROM web_events
    WHERE event_time >= '2023-11-01'
    GROUP BY session_id
)
SELECT s.started_at, ss.min_duration_sec
FROM sessions s
JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.min_duration_sec >= 180
ORDER BY started_at;
