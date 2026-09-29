SELECT id, duration_sec FROM web_events WHERE duration_sec > (SELECT AVG(duration_sec) FROM web_events WHERE event_type IN ('page_view', 'search', 'purchase'));
