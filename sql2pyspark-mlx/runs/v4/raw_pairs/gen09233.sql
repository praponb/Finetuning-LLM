SELECT
    we.id,
    we.page,
    we.event_time,
    we.duration_sec
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE HOUR(started_at) BETWEEN 11 AND 15);
