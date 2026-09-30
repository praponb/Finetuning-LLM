SELECT s.id, s.end_date, s.start_date, c.email, c.is_active FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id ORDER BY s.id;
