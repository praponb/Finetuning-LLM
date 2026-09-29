SELECT s.customer_id, s.status, s.start_date, c.signup_date FROM subscriptions s LEFT OUTER JOIN customers c ON s.customer_id = c.id;
