SELECT s.id, COUNT(we.id) web_event_count
FROM sessions s
LEFT JOIN web_events we ON we.session_id = s.id
WHERE s.customer_id IN (16, 23, 25)
GROUP BY s.id
ORDER BY web_event_count ASC, id ASC
LIMIT 3;
