SELECT s.country, s.customer_id, s.browser, we.duration_sec FROM sessions s LEFT JOIN web_events we ON s.id = we.session_id ORDER BY s.browser
