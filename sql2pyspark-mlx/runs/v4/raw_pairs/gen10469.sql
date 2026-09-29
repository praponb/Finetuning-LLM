SELECT
    s.device,
    we.event_type,
    we.event_time,
    we.session_id
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
