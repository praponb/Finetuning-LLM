SELECT id, session_id, event_type, event_time FROM web_events QUALIFY ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY duration_sec DESC NULLS FIRST, id) = 1;
