SELECT
    we.id,
    we.event_type,
    we.page,
    we.duration_sec
FROM web_events we
WHERE we.session_id NOT IN (SELECT id FROM sessions WHERE browser IS NOT NULL);
