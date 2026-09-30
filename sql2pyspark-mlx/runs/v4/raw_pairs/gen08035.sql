SELECT id, session_id FROM web_events WHERE duration_sec BETWEEN 174 AND 454 UNION SELECT id, session_id FROM web_events WHERE event_type <> 'page_view' ORDER BY id
