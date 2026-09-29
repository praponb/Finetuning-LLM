SELECT event_type, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
WHERE session_id IN (2, 9, 33, 38)
GROUP BY event_type
ORDER BY 1;
