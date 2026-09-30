SELECT s.id, s.browser, s.started_at
FROM sessions s
WHERE s.browser = 'Safari' AND s.id IN (SELECT session_id FROM web_events)
