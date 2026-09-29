SELECT plan FROM subscriptions WHERE plan IN ('enterprise', 'premium', 'basic') INTERSECT SELECT plan FROM subscriptions WHERE customer_id IN (6, 18);
