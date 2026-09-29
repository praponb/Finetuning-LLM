SELECT customer_id FROM subscriptions WHERE monthly_fee <= 180 UNION SELECT customer_id FROM subscriptions WHERE plan NOT IN ('premium', 'enterprise', 'standard') ORDER BY customer_id;
