SELECT session_id, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM web_events
WHERE session_id = 2 AND duration_sec <= 231 AND page != '/checkout'
GROUP BY session_id
ORDER BY session_id
LIMIT 1;
