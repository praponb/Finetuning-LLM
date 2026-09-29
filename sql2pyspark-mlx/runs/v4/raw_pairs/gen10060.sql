SELECT
    id,
    event_time,
    page,
    event_type,
    SUBSTRING(page, 1, 4) AS page_prefix
FROM web_events
WHERE duration_sec BETWEEN 127 AND 447
ORDER BY event_type
