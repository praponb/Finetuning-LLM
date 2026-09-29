SELECT s.browser, we.event_time FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE we.event_time >= '2023-10-01';
