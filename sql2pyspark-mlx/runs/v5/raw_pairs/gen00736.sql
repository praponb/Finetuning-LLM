SELECT s.started_at, we.page, we.duration_sec
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
WHERE s.country = 'DE';
