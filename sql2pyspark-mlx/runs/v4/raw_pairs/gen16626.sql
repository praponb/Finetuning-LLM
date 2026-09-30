SELECT we.id, we.event_type, we.event_time
FROM web_events we
WHERE HOUR(we.event_time) BETWEEN 10 AND 14 AND we.session_id NOT IN (SELECT id FROM sessions WHERE device = 'mobile');
