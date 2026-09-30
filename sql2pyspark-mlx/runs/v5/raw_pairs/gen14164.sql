SELECT s.browser, s.started_at, c.country, c.credit_limit, c.is_active FROM sessions s JOIN customers c ON s.customer_id = c.id WHERE c.is_active = TRUE
