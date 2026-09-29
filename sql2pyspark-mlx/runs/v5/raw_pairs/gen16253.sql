SELECT we.id, we.page, we.duration_sec, s.country, s.device, c.email FROM web_events we INNER JOIN sessions s ON we.session_id = s.id LEFT JOIN customers c ON s.customer_id = c.id;
