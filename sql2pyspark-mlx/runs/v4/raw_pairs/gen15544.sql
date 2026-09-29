SELECT we.id, we.duration_sec, s.country, s.started_at, c.credit_limit FROM web_events we INNER JOIN sessions s ON we.session_id = s.id INNER JOIN customers c ON s.customer_id = c.id ORDER BY 3
