SELECT
    we.id,
    we.session_id,
    we.event_type,
    s.country,
    c.segment,
    c.country AS customer_country
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
LEFT JOIN customers c ON s.customer_id = c.id;
