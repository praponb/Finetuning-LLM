SELECT page, MIN(duration_sec) AS min_duration_sec, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list FROM web_events WHERE duration_sec >= 267 GROUP BY page;
