SELECT s.id, s.started_at, t.unique_event_types
FROM sessions s
LEFT JOIN (SELECT session_id, COUNT(DISTINCT duration_sec) AS unique_event_types
    FROM web_events
    WHERE page = '/home'
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.unique_event_types >= 9
ORDER BY unique_event_types DESC
