SELECT
    id,
    page,
    event_type,
    duration_sec
FROM web_events
WHERE NOT (duration_sec <= 429 AND page IN ('/home', '/product'))
