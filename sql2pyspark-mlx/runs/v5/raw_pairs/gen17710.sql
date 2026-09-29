SELECT s.id, s.start_date, s.plan
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE email LIKE '%@mail.co.th');
