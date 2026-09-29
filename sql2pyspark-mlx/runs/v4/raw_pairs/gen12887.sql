SELECT
    s.country,
    s.started_at,
    s.device,
    we.event_time,
    we.session_id,
    we.event_type
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
WHERE we.event_time < '2024-02-01'
ORDER BY event_time;
