SELECT
    s.customer_id,
    s.started_at,
    we.event_time,
    we.page,
    we.session_id
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE s.customer_id IN (6, 8, 27);
