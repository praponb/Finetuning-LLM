SELECT *, ROUND(duration_sec * 1.15, 2) AS duration_sec_scaled FROM web_events WHERE event_type = 'click' ORDER BY event_time;
