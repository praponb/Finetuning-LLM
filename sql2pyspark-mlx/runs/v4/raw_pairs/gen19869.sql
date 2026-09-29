SELECT
    s.device,
    s.customer_id,
    s.started_at,
    we.session_id,
    we.event_type,
    we.event_time
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE we.page IN ('/search', '/cart', '/home')
