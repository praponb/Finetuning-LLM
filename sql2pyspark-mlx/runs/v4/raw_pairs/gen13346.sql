SELECT * FROM (SELECT event_type, SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) AS search_duration_sec FROM web_events GROUP BY event_type) AS sub WHERE search_duration_sec > 64
