SELECT * FROM (SELECT event_type, page, duration_sec FROM web_events WHERE duration_sec < 135) PIVOT (SUM(duration_sec) FOR page IN ('/cart', '/home', '/checkout', '/search')) ORDER BY event_type;
