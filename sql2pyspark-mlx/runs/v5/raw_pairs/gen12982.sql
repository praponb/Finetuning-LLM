SELECT
    web_events.id,
    web_events.event_time,
    web_events.duration_sec,
    web_events.page
FROM web_events
WHERE web_events.session_id IN (SELECT id FROM sessions WHERE country IN ('TH', 'SG'))
ORDER BY id DESC
