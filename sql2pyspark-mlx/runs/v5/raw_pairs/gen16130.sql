SELECT page, MIN(event_time) first_event_time, COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) add_to_cart_count
FROM web_events
WHERE duration_sec = 223 AND page IN ('/cart', '/search') AND HOUR(event_time) BETWEEN 12 AND 16
GROUP BY page
ORDER BY page;
