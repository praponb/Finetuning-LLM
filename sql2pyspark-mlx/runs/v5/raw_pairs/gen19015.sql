SELECT *, ROUND(duration_sec * 100, 2) AS duration_sec_scaled, DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label FROM web_events ORDER BY duration_sec_scaled, id LIMIT 20;
