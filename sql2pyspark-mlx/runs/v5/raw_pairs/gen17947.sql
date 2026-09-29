SELECT
    id,
    event_type,
    page,
    LENGTH(page) AS page_length
FROM web_events;
