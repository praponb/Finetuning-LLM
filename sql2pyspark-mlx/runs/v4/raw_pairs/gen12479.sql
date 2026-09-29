SELECT c.id, c.age FROM customers c WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.plan = 'premium') ORDER BY id;
