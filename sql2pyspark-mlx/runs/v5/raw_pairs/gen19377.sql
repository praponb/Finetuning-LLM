SELECT we.id, we.event_time, we.event_type FROM web_events we WHERE we.session_id IN (SELECT id FROM sessions WHERE country IS NULL);
