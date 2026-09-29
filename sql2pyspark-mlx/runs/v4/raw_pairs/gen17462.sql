SELECT
    we.id,
    we.event_time,
    we.page,
    s.started_at,
    s.browser,
    s.device
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE s.browser = 'Chrome'
ORDER BY s.browser ASC, id ASC
LIMIT 50
