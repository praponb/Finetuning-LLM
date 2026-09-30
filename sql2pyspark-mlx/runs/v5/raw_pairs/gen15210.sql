SELECT sessions.id, sessions.customer_id, sessions.country
FROM sessions
WHERE sessions.id IN (SELECT session_id FROM web_events WHERE page IN ('/cart', '/checkout'));
