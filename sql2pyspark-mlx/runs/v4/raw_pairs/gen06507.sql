SELECT duration_sec, CAST(duration_sec AS STRING) AS duration_sec_num FROM web_events WHERE (session_id = 28 AND TO_DATE(event_time) >= '2025-03-01') OR session_id = 18;
