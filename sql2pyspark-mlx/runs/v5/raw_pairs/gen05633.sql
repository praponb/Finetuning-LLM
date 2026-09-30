SELECT s.device, SORT_ARRAY(COLLECT_SET(we.event_type)) AS event_type_list, SUM(we.duration_sec) AS total_duration_sec FROM web_events we JOIN sessions s ON we.session_id = s.id GROUP BY s.device;
