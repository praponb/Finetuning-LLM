SELECT * FROM (SELECT id, session_id, duration_sec, event_time, DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS rn FROM web_events) AS r WHERE rn <= 3
