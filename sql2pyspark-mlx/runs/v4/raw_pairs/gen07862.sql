SELECT c.id, c.name, c.credit_limit FROM customers c WHERE EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.monthly_fee < 180) ORDER BY name DESC
