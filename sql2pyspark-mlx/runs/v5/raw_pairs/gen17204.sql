SELECT c.id, c.credit_limit, c.email, c.city FROM customers c WHERE c.id IN (SELECT customer_id FROM subscriptions WHERE status IN ('EXPIRED', 'CANCELLED')) ORDER BY c.city ASC;
