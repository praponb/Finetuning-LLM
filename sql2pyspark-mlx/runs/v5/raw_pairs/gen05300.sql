SELECT we.id, we.event_time, s.device FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id WHERE s.customer_id IS NOT NULL
