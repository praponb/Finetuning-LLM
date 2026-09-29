SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.device
FROM sessions s
WHERE HOUR(s.started_at) BETWEEN 6 AND 13 AND EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.event_type <> 'click')
ORDER BY s.customer_id DESC
