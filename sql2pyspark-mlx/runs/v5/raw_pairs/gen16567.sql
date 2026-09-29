SELECT s.id, s.monthly_fee, c.city, c.email FROM subscriptions s RIGHT JOIN customers c ON c.id = s.customer_id;
