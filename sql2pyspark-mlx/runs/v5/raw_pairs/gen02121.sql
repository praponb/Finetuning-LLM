SELECT s.id, s.monthly_fee FROM subscriptions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE is_active = FALSE)
