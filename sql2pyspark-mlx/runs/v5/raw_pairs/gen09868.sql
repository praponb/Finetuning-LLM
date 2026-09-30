SELECT QUARTER(event_time) AS event_quarter, event_type, ROUND((100 * SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search
FROM web_events
WHERE duration_sec <= 126
GROUP BY event_quarter, event_type
HAVING ROUND((100 * SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 79
