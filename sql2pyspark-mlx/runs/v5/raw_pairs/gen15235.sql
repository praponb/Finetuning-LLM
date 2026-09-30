WITH web_events_by_session AS (
    SELECT session_id, SUM(duration_sec) AS total_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT s.id, s.device, webs.total_duration_sec
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
ORDER BY s.id DESC
LIMIT 50;
