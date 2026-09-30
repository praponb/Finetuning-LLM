SELECT s.id, s.monthly_fee, s.customer_id FROM subscriptions s WHERE s.plan = 'basic' AND s.customer_id IN (SELECT id FROM customers WHERE MONTH(signup_date) IN (1, 2, 4));
