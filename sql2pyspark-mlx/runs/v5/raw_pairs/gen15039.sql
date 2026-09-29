SELECT s.id, s.started_at, s.customer_id, s.device, c.credit_limit FROM sessions s INNER JOIN customers c ON s.customer_id = c.id WHERE s.browser <> 'Chrome';
