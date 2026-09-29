SELECT
    s.id,
    s.browser,
    s.started_at,
    s.country
FROM sessions s
WHERE s.id IN (SELECT session_id FROM web_events WHERE duration_sec = 329);
