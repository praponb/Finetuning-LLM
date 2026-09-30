SELECT c.id, c.name, c.segment FROM customers c WHERE c.country NOT IN ('DE', 'SG', 'US') AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id) ORDER BY c.name
