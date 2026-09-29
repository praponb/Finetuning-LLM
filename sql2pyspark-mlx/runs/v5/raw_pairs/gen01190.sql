SELECT
    web_events.id,
    web_events.event_time,
    web_events.event_type,
    web_events.page,
    sessions.country,
    sessions.customer_id,
    sessions.started_at
FROM web_events
LEFT OUTER JOIN sessions ON web_events.session_id = sessions.id;
