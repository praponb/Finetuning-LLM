SELECT id, event_time, duration_sec, LEAD(duration_sec, 1, 0) OVER (ORDER BY event_time, id) AS next_duration_sec FROM web_events
