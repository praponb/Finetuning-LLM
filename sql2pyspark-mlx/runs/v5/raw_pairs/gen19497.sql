SELECT id, duration_sec, DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time FROM web_events WHERE page != '/search' AND event_time < '2025-05-21'
