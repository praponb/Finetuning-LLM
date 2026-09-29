SELECT page
FROM web_events
WHERE event_type NOT IN ('click', 'add_to_cart', 'purchase')
ORDER BY page DESC;
