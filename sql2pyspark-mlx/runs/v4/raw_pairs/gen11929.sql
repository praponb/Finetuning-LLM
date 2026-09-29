SELECT
    sessions.browser,
    sessions.device,
    sessions.started_at,
    web_events.session_id,
    web_events.page,
    web_events.event_type,
    LOWER(sessions.device) AS device_lower
FROM sessions
INNER JOIN web_events ON web_events.session_id = sessions.id
