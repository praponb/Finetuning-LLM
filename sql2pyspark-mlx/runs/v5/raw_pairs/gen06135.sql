SELECT
    we.session_id,
    we.duration_sec,
    s.customer_id,
    s.device,
    c.credit_limit
FROM web_events we
JOIN sessions s ON s.id = we.session_id
JOIN customers c ON c.id = s.customer_id
WHERE NOT (we.page = '/checkout' AND we.duration_sec = 295)
