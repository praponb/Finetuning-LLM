SELECT we.id, we.session_id, s.device, s.customer_id, s.browser FROM web_events we INNER JOIN sessions s ON s.id = we.session_id;
