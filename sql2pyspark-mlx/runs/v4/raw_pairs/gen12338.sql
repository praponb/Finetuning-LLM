SELECT
    we.id,
    we.event_type,
    we.session_id,
    s.customer_id,
    c.city
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.age IS NOT NULL;
