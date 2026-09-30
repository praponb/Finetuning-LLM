SELECT *, CONCAT(page, '/', event_type) AS page_event_type
FROM web_events
WHERE event_type = 'add_to_cart'
