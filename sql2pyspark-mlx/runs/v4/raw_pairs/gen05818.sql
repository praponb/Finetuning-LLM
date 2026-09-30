SELECT event_type, session_id, HOUR(event_time) AS event_hour, ROUND((100 * SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search FROM web_events GROUP BY 1, 2, 3;
