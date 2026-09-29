SELECT id, event_time, page, duration_sec, DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time, SUBSTRING(event_type, 1, 4) AS event_type_prefix FROM web_events
