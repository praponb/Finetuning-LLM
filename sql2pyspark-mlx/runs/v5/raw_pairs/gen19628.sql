SELECT sessions.customer_id, web_events.event_time, web_events.page
FROM sessions
LEFT JOIN web_events ON sessions.id = web_events.session_id
ORDER BY customer_id DESC, event_time
