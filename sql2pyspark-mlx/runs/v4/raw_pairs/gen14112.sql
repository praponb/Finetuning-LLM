SELECT s.id, s.country
FROM sessions s
WHERE s.country IN ('DE', 'JP', 'US') AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.duration_sec = 377)
