SELECT MIN(event_time) AS min_event_time, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range FROM web_events ORDER BY duration_sec_range DESC
