SELECT DISTINCT event_type, page FROM web_events WHERE TO_DATE(event_time) >= '2025-06-19';
