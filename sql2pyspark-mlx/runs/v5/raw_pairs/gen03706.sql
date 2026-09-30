SELECT session_id FROM web_events WHERE duration_sec != 339 UNION SELECT session_id FROM web_events WHERE TO_DATE(event_time) < '2023-10-01' ORDER BY session_id;
