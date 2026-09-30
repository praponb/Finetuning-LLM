SELECT
    web_events.session_id,
    web_events.event_type,
    web_events.duration_sec,
    sessions.country,
    sessions.browser,
    sessions.started_at
FROM web_events
INNER JOIN sessions ON web_events.session_id = sessions.id
WHERE sessions.device = 'tablet'
ORDER BY sessions.country DESC, started_at
