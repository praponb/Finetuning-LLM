SELECT
    we.id,
    we.event_type,
    we.duration_sec,
    we.page,
    s.browser,
    s.customer_id,
    s.started_at
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE TO_DATE(we.event_time) < '2023-04-01'
ORDER BY event_type DESC
