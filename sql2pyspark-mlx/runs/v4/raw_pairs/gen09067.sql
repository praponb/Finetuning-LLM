SELECT id, event_time, event_type, TRIM(event_type) AS clean_event_type, DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time FROM web_events
