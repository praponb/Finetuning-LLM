SELECT s.id, s.plan, s.status, c.signup_date, c.city, c.segment FROM subscriptions s INNER JOIN customers c ON c.id = s.customer_id ORDER BY id LIMIT 10;
