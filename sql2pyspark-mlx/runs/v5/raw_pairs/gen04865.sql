SELECT SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count, COUNT(duration_sec) AS duration_sec_count FROM web_events;
