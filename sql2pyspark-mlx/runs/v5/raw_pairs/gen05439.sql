SELECT s.id, s.device
FROM sessions s
WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec BETWEEN 134 AND 350)
ORDER BY s.device
