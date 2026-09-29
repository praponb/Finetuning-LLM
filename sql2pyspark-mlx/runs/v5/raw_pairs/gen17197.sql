SELECT
    we.session_id,
    we.event_type,
    s.browser,
    s.customer_id,
    c.signup_date,
    c.name
FROM web_events we
JOIN sessions s ON we.session_id = s.id
JOIN customers c ON s.customer_id = c.id;
