SELECT s.start_date, c.country, c.email FROM subscriptions s LEFT JOIN customers c ON s.customer_id = c.id
