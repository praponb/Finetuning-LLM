SELECT s.id, COUNT(we.id) AS web_event_count FROM sessions s JOIN web_events we ON s.id = we.session_id WHERE we.duration_sec >= 341 GROUP BY s.id ORDER BY id, web_event_count DESC
