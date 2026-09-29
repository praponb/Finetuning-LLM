SELECT
    we.id,
    we.event_type,
    we.event_time,
    s.device,
    s.customer_id,
    c.email
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.name IS NULL;
