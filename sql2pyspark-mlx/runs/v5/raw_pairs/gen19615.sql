SELECT
    sessions.id,
    sessions.country,
    sessions.device,
    t.avg_duration_sec
FROM sessions
JOIN (SELECT session_id, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id) t ON sessions.id = t.session_id
WHERE t.avg_duration_sec >= 350
ORDER BY sessions.country ASC, id ASC
LIMIT 1
