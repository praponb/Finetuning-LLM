SELECT session_id, duration_sec, ROUND(duration_sec * 0.85, 2) AS adjusted_duration_sec FROM web_events WHERE page IN ('/checkout', '/cart') AND session_id IN (1, 21, 36, 40);
