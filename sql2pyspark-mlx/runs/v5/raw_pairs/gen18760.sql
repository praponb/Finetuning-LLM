SELECT * FROM (SELECT session_id, page, duration_sec FROM web_events WHERE session_id IN (23, 25, 28))
PIVOT (COUNT(duration_sec) FOR page IN ('/home', '/search', '/checkout', '/cart'))
ORDER BY session_id;
