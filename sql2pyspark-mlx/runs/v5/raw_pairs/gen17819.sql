SELECT DATE_TRUNC('MONTH', event_time) AS event_month, SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END) AS search_count FROM web_events WHERE duration_sec IS NULL GROUP BY event_month
