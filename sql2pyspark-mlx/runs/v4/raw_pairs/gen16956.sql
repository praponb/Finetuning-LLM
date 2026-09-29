SELECT event_type, MIN(duration_sec) AS min_duration_sec
FROM web_events
WHERE session_id IN (18, 22, 24, 29)
GROUP BY event_type;
