SELECT page, LENGTH(event_type) AS event_type_length, DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time
FROM web_events
WHERE event_type = 'page_view' AND session_id IN (18, 35) AND session_id IN (4, 10, 12)
ORDER BY days_since_event_time DESC, event_type_length ASC;
