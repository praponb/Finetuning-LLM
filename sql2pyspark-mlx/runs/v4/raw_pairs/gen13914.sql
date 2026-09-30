SELECT
    web_events.id,
    web_events.page,
    sessions.country,
    sessions.browser,
    sessions.started_at
FROM web_events
INNER JOIN sessions ON web_events.session_id = sessions.id
