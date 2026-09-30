SELECT we.duration_sec, we.event_time, s.country, s.browser, s.customer_id FROM web_events we FULL OUTER JOIN sessions s ON s.id = we.session_id WHERE we.duration_sec = 232 ORDER BY event_time
