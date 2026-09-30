SELECT
    id,
    event_time,
    CEIL(duration_sec) AS duration_sec_whole,
    DAY(event_time) AS event_time_day
FROM web_events
WHERE duration_sec BETWEEN 194 AND 416
ORDER BY 3 DESC, id DESC
LIMIT 50;
