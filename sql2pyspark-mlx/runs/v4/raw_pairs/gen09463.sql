SELECT s.id, s.country, s.browser
FROM sessions s
WHERE s.country IS NULL AND s.id IN (SELECT session_id FROM web_events)
