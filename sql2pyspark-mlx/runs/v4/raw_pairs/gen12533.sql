SELECT MONTH(event_time) AS event_month, MIN(duration_sec) AS min_duration_sec
FROM web_events
GROUP BY MONTH(event_time)
