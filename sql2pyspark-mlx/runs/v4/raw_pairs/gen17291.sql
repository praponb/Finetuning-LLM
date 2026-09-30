SELECT sessions.country, sessions.customer_id, sessions.browser, web_events.event_time, web_events.page FROM sessions LEFT JOIN web_events ON sessions.id = web_events.session_id;
