SELECT id, duration_sec
FROM web_events
WHERE page IN ('/home', '/cart', '/checkout')
UNION
SELECT id, duration_sec
FROM web_events
WHERE duration_sec < 290
ORDER BY duration_sec;
