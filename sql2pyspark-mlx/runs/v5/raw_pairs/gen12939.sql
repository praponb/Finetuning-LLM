SELECT id, start_date
FROM subscriptions
WHERE start_date >= (SELECT MAX(start_date) FROM subscriptions WHERE customer_id IN (14, 19, 39, 40))
ORDER BY id;
