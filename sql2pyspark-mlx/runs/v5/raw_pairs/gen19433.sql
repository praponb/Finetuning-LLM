SELECT
    id,
    duration_sec,
    event_time,
    event_type,
    COALESCE(page, 'Unknown') page_filled
FROM web_events
WHERE page = '/product'
