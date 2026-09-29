SELECT *, UPPER(event_type) AS event_type_upper
FROM web_events
WHERE event_time < '2025-05-18' AND (session_id IN (8, 19) OR duration_sec <> 444);
