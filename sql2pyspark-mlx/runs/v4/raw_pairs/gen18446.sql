SELECT
    id,
    page,
    event_type,
    session_id
FROM web_events
WHERE event_type = 'search'
ORDER BY page;
