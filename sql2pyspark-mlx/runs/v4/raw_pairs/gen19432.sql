SELECT s.id, s.start_date, s.plan, s.status FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE name LIKE '%Ivy%') ORDER BY s.plan
