SELECT page, event_type, MIN(event_time) AS first_event_time FROM web_events GROUP BY ROLLUP(page, event_type)
