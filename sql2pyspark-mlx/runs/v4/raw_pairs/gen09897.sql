SELECT event_type, session_id FROM web_events WHERE page = '/cart' AND event_type = 'click' AND event_type IN ('page_view', 'search')
