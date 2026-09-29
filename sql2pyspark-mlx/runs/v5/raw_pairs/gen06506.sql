SELECT QUARTER(event_time) AS event_quarter, MAX(event_time) AS last_event_time FROM web_events WHERE session_id IN (12, 15, 20, 39) GROUP BY event_quarter ORDER BY last_event_time;
