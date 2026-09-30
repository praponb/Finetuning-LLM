SELECT
    we.id,
    we.event_time,
    we.event_type,
    s.browser,
    c.name
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id;
