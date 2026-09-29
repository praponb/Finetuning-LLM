SELECT we.id, we.duration_sec, we.event_type FROM web_events we WHERE we.page IN ('/checkout', '/search') AND we.session_id IN (SELECT id FROM sessions WHERE country IN ('UK', 'DE'));
