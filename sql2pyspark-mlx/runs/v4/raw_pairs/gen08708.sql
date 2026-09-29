SELECT
    s.id,
    s.started_at,
    s.customer_id,
    s.country
FROM sessions s
WHERE NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
ORDER BY s.country DESC, id
LIMIT 3;
