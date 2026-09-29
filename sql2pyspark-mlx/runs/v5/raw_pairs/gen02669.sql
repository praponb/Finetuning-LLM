SELECT id, page, session_id, event_time, YEAR(event_time) AS event_time_year FROM web_events WHERE session_id IN (14, 25, 34) AND session_id IN (18, 32) AND event_type IN ('click', 'page_view')
