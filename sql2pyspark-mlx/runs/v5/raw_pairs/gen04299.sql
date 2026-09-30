SELECT we.id, we.event_time, we.event_type, we.session_id FROM web_events we WHERE we.page = '/search' AND we.session_id IN (SELECT id FROM sessions WHERE customer_id IN (14, 32, 38));
