SELECT
    we.event_type,
    we.page,
    s.country,
    s.customer_id,
    s.device
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
