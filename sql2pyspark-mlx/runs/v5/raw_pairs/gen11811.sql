SELECT event_hour, avg_duration_sec, duration_sec_count
FROM (
    SELECT HOUR(event_time) event_hour, AVG(duration_sec) avg_duration_sec, COUNT(duration_sec) duration_sec_count
    FROM web_events
    GROUP BY HOUR(event_time)
) s
WHERE avg_duration_sec >= 390
