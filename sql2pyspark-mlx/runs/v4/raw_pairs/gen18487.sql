SELECT
    s.started_at,
    s.customer_id,
    s.device,
    we.session_id
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
WHERE s.country = 'UK'
