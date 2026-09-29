SELECT page, duration_sec FROM web_events WHERE session_id IN (3, 32, 36) UNION ALL SELECT page, duration_sec FROM web_events WHERE duration_sec >= 402;
