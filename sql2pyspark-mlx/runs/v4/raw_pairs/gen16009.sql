SELECT id, session_id, event_time, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) AS event_time_row FROM web_events
