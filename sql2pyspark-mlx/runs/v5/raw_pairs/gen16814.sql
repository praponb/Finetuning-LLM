WITH session_totals AS (
    SELECT session_id, SUM(duration_sec) AS total_duration_sec, COUNT(DISTINCT duration_sec) AS unique_event_types
    FROM web_events
    GROUP BY session_id
)
SELECT
    sessions.id,
    sessions.started_at,
    session_totals.total_duration_sec,
    session_totals.unique_event_types
FROM sessions
INNER JOIN session_totals ON sessions.id = session_totals.session_id
WHERE session_totals.unique_event_types > 5
ORDER BY total_duration_sec ASC, id ASC
LIMIT 10;
