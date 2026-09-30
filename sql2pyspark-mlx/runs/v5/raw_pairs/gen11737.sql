SELECT
    web_events.id,
    web_events.page,
    web_events.event_type,
    web_events.duration_sec
FROM web_events
WHERE web_events.page IN ('/product', '/home', '/search') AND web_events.session_id IN (SELECT id FROM sessions WHERE customer_id IS NOT NULL)
