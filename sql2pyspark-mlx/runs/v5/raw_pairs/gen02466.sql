SELECT we.id, we.event_type FROM web_events we WHERE we.page IN ('/product', '/search', '/checkout') AND we.session_id IN (SELECT id FROM sessions WHERE started_at >= '2023-08-01') ORDER BY id DESC;
