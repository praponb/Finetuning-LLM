SELECT
    sessions.browser,
    web_events.event_type,
    web_events.page,
    web_events.session_id
FROM sessions
LEFT JOIN web_events ON sessions.id = web_events.session_id
