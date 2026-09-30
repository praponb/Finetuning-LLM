SELECT s.id, s.customer_id, s.monthly_fee, s.end_date FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE country NOT IN ('SG', 'JP', 'TH')) ORDER BY id LIMIT 50
