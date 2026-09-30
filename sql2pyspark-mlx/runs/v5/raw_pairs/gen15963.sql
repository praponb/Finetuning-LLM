SELECT s.id, s.browser, COUNT(we.id) AS web_event_count
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id
WHERE we.page IN ('/home', '/cart')
GROUP BY s.id, s.browser
HAVING COUNT(we.id) > 2
ORDER BY id, web_event_count DESC;
