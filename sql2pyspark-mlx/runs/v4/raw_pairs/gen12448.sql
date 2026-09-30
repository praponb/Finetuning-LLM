SELECT s.id, s.browser, COUNT(we.id) AS web_event_count
FROM sessions s
LEFT JOIN web_events we ON s.id = we.session_id
GROUP BY s.id, s.browser
HAVING COUNT(we.id) > 4
