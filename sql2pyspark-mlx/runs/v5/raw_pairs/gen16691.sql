SELECT sessions.started_at, t.home_duration_sec
FROM sessions
INNER JOIN (SELECT session_id, SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) AS home_duration_sec
    FROM web_events
    WHERE session_id IN (26, 28, 35)
    GROUP BY session_id) t ON sessions.id = t.session_id
WHERE t.home_duration_sec > 75
ORDER BY home_duration_sec;
