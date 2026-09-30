SELECT s.id, s.monthly_fee
FROM subscriptions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit BETWEEN 9500 AND 15100)
ORDER BY id DESC;
