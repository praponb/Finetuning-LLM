SELECT
    s.started_at,
    s.device,
    s.country,
    we.duration_sec,
    we.event_type,
    we.session_id
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
