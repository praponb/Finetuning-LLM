SELECT s.id, s.customer_id, s.browser, s.started_at, c.name, c.country FROM sessions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.credit_limit <= 11500 ORDER BY s.id;
