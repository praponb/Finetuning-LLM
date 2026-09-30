SELECT id, event_type, CONCAT(event_type, '/', page) AS event_type_page FROM web_events WHERE event_time < '2023-03-01' ORDER BY id;
