SELECT id, event_type, FLOOR(duration_sec) AS duration_sec_whole
FROM web_events
WHERE session_id IN (13, 33, 36, 39)
ORDER BY 1
