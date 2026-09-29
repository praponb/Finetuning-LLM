SELECT s.id, s.status, s.start_date, c.name, c.age FROM subscriptions s LEFT JOIN customers c ON s.customer_id = c.id
