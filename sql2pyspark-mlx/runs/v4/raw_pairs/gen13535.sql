SELECT c.id, c.segment FROM customers c WHERE c.is_active = FALSE AND EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.status != 'ACTIVE') ORDER BY c.id DESC
