SELECT id, duration_sec FROM web_events WHERE event_time >= '2025-06-01' AND duration_sec <= 271 ORDER BY id LIMIT 50;
