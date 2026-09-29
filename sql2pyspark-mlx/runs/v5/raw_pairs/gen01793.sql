SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS row_num,
    SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id) AS running_duration_sec
FROM web_events;
