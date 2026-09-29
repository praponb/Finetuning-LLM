SELECT
    we.id,
    we.event_time,
    we.duration_sec,
    s.started_at
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
WHERE we.page = '/search'
ORDER BY we.duration_sec DESC, s.started_at;
