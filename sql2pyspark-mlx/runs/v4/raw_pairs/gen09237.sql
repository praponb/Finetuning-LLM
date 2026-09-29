SELECT id, page, event_time, NTILE(4) OVER (PARTITION BY page ORDER BY event_time, id) AS event_time_ntile FROM web_events;
