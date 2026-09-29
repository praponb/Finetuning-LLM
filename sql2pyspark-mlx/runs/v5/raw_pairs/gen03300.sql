SELECT s.id, s.started_at, s.country
FROM sessions s
WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id IN (28, 30, 31))
ORDER BY country, s.started_at
