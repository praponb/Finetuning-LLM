SELECT id, event_time, duration_sec, TO_DATE(event_time) AS event_time_date FROM web_events WHERE duration_sec >= 142 AND (duration_sec = 353 OR event_type IN ('search', 'page_view', 'purchase'))
