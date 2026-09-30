SELECT DISTINCT event_type, page FROM web_events WHERE event_type IN ('search', 'purchase', 'page_view');
