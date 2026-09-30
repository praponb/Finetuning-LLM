SELECT MONTH(event_time) AS event_month, MIN(duration_sec) AS min_duration_sec FROM web_events WHERE duration_sec >= 448 GROUP BY event_month ORDER BY event_month, min_duration_sec DESC LIMIT 5;
