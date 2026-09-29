SELECT s.customer_id, we.duration_sec, we.event_time, we.session_id FROM sessions s LEFT JOIN web_events we ON s.id = we.session_id WHERE s.device <> 'mobile' ORDER BY session_id, duration_sec
