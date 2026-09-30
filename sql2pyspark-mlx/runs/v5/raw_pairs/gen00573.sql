SELECT QUARTER(event_time) event_quarter, COUNT(duration_sec) duration_sec_count, MIN(duration_sec) min_duration_sec
FROM web_events
WHERE event_time < '2024-12-01'
GROUP BY 1
ORDER BY min_duration_sec;
