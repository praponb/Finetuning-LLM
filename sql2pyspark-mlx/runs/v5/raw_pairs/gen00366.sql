SELECT id, session_id, event_time, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) AS rn FROM web_events WHERE duration_sec IS NOT NULL ORDER BY event_time;
