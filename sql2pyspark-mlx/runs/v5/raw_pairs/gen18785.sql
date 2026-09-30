SELECT
    we.id,
    we.event_type,
    we.duration_sec,
    s.country,
    s.browser,
    s.device
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
ORDER BY country, id
LIMIT 5;
