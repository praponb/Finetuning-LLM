SELECT s.id, s.customer_id, c.city, c.name, c.signup_date FROM subscriptions s JOIN customers c ON s.customer_id = c.id;
