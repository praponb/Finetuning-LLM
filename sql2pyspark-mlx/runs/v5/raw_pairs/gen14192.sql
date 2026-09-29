SELECT we.id, we.duration_sec, we.page, s.country, s.device, c.email FROM web_events we INNER JOIN sessions s ON s.id = we.session_id INNER JOIN customers c ON s.customer_id = c.id WHERE c.is_active;
