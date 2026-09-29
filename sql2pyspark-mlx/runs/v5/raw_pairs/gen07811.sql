SELECT
    we.id,
    we.event_type,
    we.session_id,
    we.event_time
FROM web_events we
WHERE we.page = '/cart' AND we.session_id IN (SELECT id FROM sessions WHERE country NOT IN ('US', 'SG'))
ORDER BY we.id DESC
LIMIT 1
