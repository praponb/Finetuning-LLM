SELECT *
FROM web_events
WHERE event_time >= '2023-07-01' AND event_time < '2023-12-01' AND page IN ('/home', '/product')
ORDER BY event_time DESC, id ASC
LIMIT 50;
