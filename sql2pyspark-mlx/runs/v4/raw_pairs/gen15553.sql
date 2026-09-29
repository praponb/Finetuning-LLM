SELECT
    web_events.id,
    web_events.duration_sec,
    web_events.event_type,
    sessions.customer_id
FROM web_events
FULL OUTER JOIN sessions ON web_events.session_id = sessions.id
WHERE web_events.page = '/checkout' AND web_events.duration_sec <= 311;
