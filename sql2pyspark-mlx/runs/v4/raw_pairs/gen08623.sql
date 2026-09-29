SELECT id, session_id, DATE_FORMAT(event_time, 'yyyy-MM') event_time_label
FROM web_events
WHERE event_type IN ('page_view', 'click') AND duration_sec < 341
