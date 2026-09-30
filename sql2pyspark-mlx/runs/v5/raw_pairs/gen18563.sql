SELECT id, page, event_time, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS event_time_row FROM web_events WHERE duration_sec > 431
