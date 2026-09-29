SELECT we.id, we.event_time, we.event_type, we.session_id, s.customer_id FROM web_events we LEFT OUTER JOIN sessions s ON s.id = we.session_id ORDER BY session_id DESC, customer_id DESC
