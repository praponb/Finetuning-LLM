SELECT
    id,
    page,
    event_time,
    event_type,
    duration_sec,
    RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS rnk,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS rn
FROM web_events
WHERE event_type = 'search';
