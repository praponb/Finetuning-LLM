SELECT s.id, s.country, COUNT(we.id) AS web_event_count
FROM sessions s
LEFT OUTER JOIN web_events we ON s.id = we.session_id
WHERE we.page IN ('/home', '/cart', '/search')
GROUP BY s.id, s.country
HAVING COUNT(we.id) > 5;
