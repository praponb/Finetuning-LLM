SELECT
    s.id,
    s.started_at,
    s.customer_id,
    s.device
FROM sessions s
WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_type IN ('add_to_cart', 'page_view'));
