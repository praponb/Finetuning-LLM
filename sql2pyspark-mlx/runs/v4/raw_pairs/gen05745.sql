SELECT s.id, s.monthly_fee FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE name LIKE 'FRA%');
