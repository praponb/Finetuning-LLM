SELECT s.id, s.browser, s.device, s.started_at FROM sessions s WHERE s.id IN (SELECT session_id FROM web_events WHERE HOUR(event_time) BETWEEN 10 AND 16) ORDER BY started_at
