SELECT session_id, event_time, LAG(page, 1) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_page FROM web_events;
