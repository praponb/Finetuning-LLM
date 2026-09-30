SELECT event_type, ROUND((100 * SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search, COUNT(duration_sec) AS duration_sec_count FROM web_events GROUP BY event_type;
