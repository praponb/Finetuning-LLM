SELECT HOUR(event_time) event_hour, SUM(duration_sec) total_duration_sec, SUM(SUM(duration_sec)) OVER (ORDER BY HOUR(event_time)) AS cumulative_duration_sec
FROM web_events
WHERE duration_sec <> 173
GROUP BY HOUR(event_time);
