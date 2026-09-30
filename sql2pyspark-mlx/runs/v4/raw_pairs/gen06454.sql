SELECT s.id, s.country
FROM sessions s
WHERE s.browser NOT IN ('Chrome', 'Safari', 'Edge') AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.session_id IN (12, 14, 18, 32))
ORDER BY id DESC
