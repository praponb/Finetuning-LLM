SELECT
    id,
    event_type,
    page,
    event_time,
    duration_sec
FROM web_events
QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) = 1;
