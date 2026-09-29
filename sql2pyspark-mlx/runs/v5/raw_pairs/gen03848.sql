SELECT s.id, s.end_date, s.customer_id, s.start_date FROM subscriptions s WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE name LIKE '%Park%') ORDER BY end_date DESC
