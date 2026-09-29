SELECT s.id, s.device, s.started_at, c.signup_date FROM sessions s INNER JOIN customers c ON s.customer_id = c.id WHERE s.browser IN ('Safari', 'Firefox') ORDER BY started_at DESC, id LIMIT 10;
