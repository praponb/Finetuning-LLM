SELECT we.id, we.duration_sec FROM web_events we WHERE we.event_type = 'click' AND we.session_id NOT IN (SELECT id FROM sessions WHERE browser != 'Edge');
