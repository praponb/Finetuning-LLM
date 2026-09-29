SELECT page, MIN(event_time) AS min_event_time, SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS search_count FROM web_events GROUP BY page
