SELECT *
FROM web_events
WHERE event_type IN ('purchase', 'add_to_cart')
ORDER BY event_type;
