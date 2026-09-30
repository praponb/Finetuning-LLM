SELECT
    we.id,
    we.session_id,
    we.page,
    we.event_time
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE customer_id IN (17, 20, 32, 40))
