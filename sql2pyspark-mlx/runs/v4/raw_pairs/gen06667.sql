SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events)
PIVOT (AVG(duration_sec) FOR page IN ('/cart', '/checkout', '/home'))
ORDER BY event_type;
