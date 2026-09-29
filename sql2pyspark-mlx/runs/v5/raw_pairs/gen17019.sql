SELECT page, MAX(event_time) last_event_time FROM web_events WHERE TO_DATE(event_time) >= '2023-04-25' GROUP BY page;
