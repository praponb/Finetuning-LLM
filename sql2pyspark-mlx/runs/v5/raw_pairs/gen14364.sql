SELECT
    we.event_type,
    we.page,
    s.customer_id,
    s.country
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id;
