SELECT c.id, c.segment, c.is_active, c.country FROM customers c WHERE EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.monthly_fee < 220)
