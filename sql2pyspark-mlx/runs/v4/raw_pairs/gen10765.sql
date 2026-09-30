SELECT
    we.page,
    we.session_id,
    s.customer_id,
    s.started_at,
    s.browser
FROM web_events we
LEFT OUTER JOIN sessions s ON we.session_id = s.id
WHERE we.event_type = 'search'
ORDER BY browser
