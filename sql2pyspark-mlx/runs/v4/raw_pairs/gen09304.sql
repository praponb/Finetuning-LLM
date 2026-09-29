SELECT s.id, s.end_date FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE credit_limit <= 14100) ORDER BY id DESC;
