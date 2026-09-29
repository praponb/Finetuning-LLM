SELECT id, session_id, event_type, duration_sec FROM web_events QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS FIRST, id) = 1
