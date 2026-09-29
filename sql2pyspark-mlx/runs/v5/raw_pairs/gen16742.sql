SELECT
    we.id,
    we.event_type,
    s.browser,
    s.started_at,
    s.country
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
WHERE HOUR(we.event_time) BETWEEN 9 AND 14
ORDER BY 1 DESC, s.started_at DESC;
