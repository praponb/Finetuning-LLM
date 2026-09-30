SELECT
    s.browser,
    s.started_at,
    s.customer_id,
    we.page,
    we.session_id,
    we.event_type
FROM sessions s
LEFT OUTER JOIN web_events we ON we.session_id = s.id
WHERE we.duration_sec >= 265
