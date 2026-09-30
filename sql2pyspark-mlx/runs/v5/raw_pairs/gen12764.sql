SELECT sessions.id, sessions.country, sessions.device FROM sessions WHERE sessions.id IN (SELECT session_id FROM web_events WHERE page NOT IN ('/checkout', '/cart')) ORDER BY 3 DESC
