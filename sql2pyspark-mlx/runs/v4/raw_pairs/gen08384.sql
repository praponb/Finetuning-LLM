SELECT we.id, we.event_type FROM web_events we WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE browser <> 'Safari');
