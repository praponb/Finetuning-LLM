SELECT s.id, s.customer_id FROM subscriptions s WHERE s.plan = 'basic' AND s.customer_id IN (SELECT id FROM customers WHERE country NOT IN ('SG', 'DE')) ORDER BY s.customer_id DESC;
