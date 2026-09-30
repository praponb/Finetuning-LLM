SELECT s.id, s.country
FROM sessions s
WHERE s.browser IN ('Chrome', 'Edge', 'Safari') AND s.id IN (SELECT session_id FROM web_events WHERE session_id IN (5, 13, 16))
