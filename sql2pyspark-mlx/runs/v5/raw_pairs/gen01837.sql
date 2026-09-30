SELECT we.id, we.event_type, we.page
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE browser <> 'Edge')
ORDER BY event_type, id
LIMIT 10;
