SELECT we.id, we.event_type, we.page, s.customer_id, s.device, c.is_active, c.country FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id JOIN customers c ON s.customer_id = c.id
