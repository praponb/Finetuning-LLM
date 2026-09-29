SELECT
    we.id,
    we.event_type,
    we.duration_sec,
    s.device,
    s.customer_id,
    c.name,
    c.email
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
INNER JOIN customers c ON s.customer_id = c.id;
