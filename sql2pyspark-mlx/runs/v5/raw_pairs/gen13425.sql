SELECT s.id, s.plan FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE NOT is_active)
