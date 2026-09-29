SELECT id, page, event_time, duration_sec, LEAD(duration_sec, 1) OVER (PARTITION BY page ORDER BY event_time, id) AS next_duration_sec FROM web_events WHERE event_time < '2024-03-19'
