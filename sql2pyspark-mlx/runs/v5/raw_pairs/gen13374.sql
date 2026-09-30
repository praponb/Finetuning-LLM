SELECT s.id, s.status, s.customer_id, c.segment FROM subscriptions s LEFT JOIN customers c ON c.id = s.customer_id;
