SELECT
    id,
    page,
    event_type,
    session_id,
    UPPER(page) AS page_upper
FROM web_events
WHERE event_type IN ('page_view', 'click');
