SELECT id, plan
FROM subscriptions
WHERE monthly_fee BETWEEN 200 AND 240 AND customer_id IS NULL AND customer_id IN (5, 15, 26, 39);
