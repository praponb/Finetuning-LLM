SELECT id, duration_sec, page, session_id, TO_DATE(event_time) AS event_time_date FROM web_events WHERE event_time < '2024-11-01' AND duration_sec BETWEEN 264 AND 308 ORDER BY session_id;
