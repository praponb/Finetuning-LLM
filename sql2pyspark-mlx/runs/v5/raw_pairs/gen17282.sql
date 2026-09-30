SELECT
    we.id,
    we.event_type,
    we.event_time,
    s.started_at,
    s.country,
    c.age
FROM web_events we
JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON s.customer_id = c.id
