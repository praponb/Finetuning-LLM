SELECT SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
FROM web_events
ORDER BY duration_sec_range;
