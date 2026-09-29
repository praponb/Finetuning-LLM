SELECT s.browser, s.started_at, c.name FROM sessions s RIGHT JOIN customers c ON s.customer_id = c.id;
