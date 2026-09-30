SELECT COUNT(DISTINCT duration_sec) AS unique_pages, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values
FROM web_events
WHERE page = '/checkout' AND duration_sec <> 336
