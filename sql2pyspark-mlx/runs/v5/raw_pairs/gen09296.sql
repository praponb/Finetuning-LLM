SELECT s.id, s.end_date, s.status, s.customer_id, c.segment, c.is_active, c.signup_date FROM subscriptions s FULL OUTER JOIN customers c ON s.customer_id = c.id ORDER BY c.signup_date, s.status;
