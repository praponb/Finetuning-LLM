SELECT id, event_type, event_time, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS rn FROM web_events WHERE event_time < '2023-12-01' ORDER BY event_type ASC;
