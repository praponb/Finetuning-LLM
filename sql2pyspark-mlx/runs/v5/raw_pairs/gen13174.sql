SELECT sessions.id, COUNT(web_events.id) web_event_count
FROM sessions
LEFT JOIN web_events ON web_events.session_id = sessions.id
WHERE sessions.browser = 'Chrome'
GROUP BY sessions.id
HAVING COUNT(web_events.id) >= 5;
