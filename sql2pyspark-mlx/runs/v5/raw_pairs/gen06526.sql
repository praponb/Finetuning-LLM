SELECT s.browser, s.device, s.country, we.event_time, we.duration_sec FROM sessions s JOIN web_events we ON s.id = we.session_id
