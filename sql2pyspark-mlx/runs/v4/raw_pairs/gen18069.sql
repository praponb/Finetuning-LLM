SELECT
    id,
    event_type,
    event_time,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS event_time_row
FROM web_events
