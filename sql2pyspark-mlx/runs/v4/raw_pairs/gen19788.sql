SELECT web_events.id, web_events.event_type
FROM web_events
WHERE web_events.event_time >= '2023-05-18' AND web_events.session_id IN (SELECT id FROM sessions WHERE started_at < '2025-01-21')
ORDER BY event_type;
