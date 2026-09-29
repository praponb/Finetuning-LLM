SELECT
    s.country,
    s.browser,
    s.started_at,
    we.duration_sec
FROM sessions s
INNER JOIN web_events we ON we.session_id = s.id
ORDER BY s.started_at
