SELECT duration_sec, page, LOWER(event_type) AS event_type_lower FROM web_events WHERE event_time >= '2024-03-01' AND session_id IN (3, 11)
