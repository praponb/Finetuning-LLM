SELECT
    we.session_id,
    we.page,
    s.device,
    c.country
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id;
