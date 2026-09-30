SELECT
    we.id,
    we.event_type,
    we.page,
    s.customer_id,
    c.signup_date,
    c.is_active
FROM web_events we
RIGHT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id
WHERE we.duration_sec > 138 OR s.browser = 'Edge'
ORDER BY we.id DESC;
