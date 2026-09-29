SELECT * FROM (SELECT session_id, event_type, duration_sec FROM web_events WHERE event_time < '2024-04-01')
PIVOT (SUM(duration_sec) FOR event_type IN ('click', 'purchase'))
ORDER BY session_id;
