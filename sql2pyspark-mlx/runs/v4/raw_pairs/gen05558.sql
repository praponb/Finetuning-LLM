SELECT s.id, s.monthly_fee, s.plan FROM subscriptions s WHERE s.customer_id IS NOT NULL AND s.customer_id NOT IN (SELECT id FROM customers WHERE age BETWEEN 34 AND 35) ORDER BY s.monthly_fee DESC
