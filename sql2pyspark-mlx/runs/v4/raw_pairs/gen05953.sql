SELECT page
FROM web_events
EXCEPT DISTINCT
SELECT page
FROM web_events
WHERE event_type IN ('click', 'search', 'add_to_cart')
