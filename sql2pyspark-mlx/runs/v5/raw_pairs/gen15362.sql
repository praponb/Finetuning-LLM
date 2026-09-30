SELECT id, event_time, NTILE(3) OVER (ORDER BY event_time, id) AS quartile FROM web_events ORDER BY 2
