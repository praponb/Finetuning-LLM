SELECT c.country, COUNT(s.id) AS subscription_count FROM customers c LEFT JOIN subscriptions s ON c.id = s.customer_id WHERE c.is_active GROUP BY c.country;
