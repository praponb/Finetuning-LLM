SELECT CASE WHEN event_type IN ('purchase', 'add_to_cart') THEN 'priority' ELSE 'other' END event_type_group, MAX(event_time) latest_event_time
FROM web_events
WHERE (session_id = 31 AND duration_sec <= 278) OR event_type IN ('search', 'add_to_cart', 'click')
GROUP BY 1
ORDER BY event_type_group, latest_event_time
